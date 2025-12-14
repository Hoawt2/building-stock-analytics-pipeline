import os
import argparse
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, Table, MetaData
from dotenv import load_dotenv
from datetime import datetime, timedelta
from time import sleep

# ============================================================
# 1️⃣ TẠO ENGINE KẾT NỐI DATABASE
# ============================================================
DOTENV_PATH = '/opt/airflow/.env'
def get_db_engine():
    load_dotenv(DOTENV_PATH)
    
    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_db = os.getenv("MYSQL_DATABASE")
    mysql_host = os.getenv("MYSQL_HOST")
    mysql_port = os.getenv("MYSQL_PORT")
    
    if not all([mysql_user, mysql_password, mysql_db, mysql_host, mysql_port]):
        raise ValueError("Thiếu hoặc sai thông tin kết nối database trong file .env")
    
    connection_string = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}"
    engine = create_engine(connection_string)
    return engine 

# ============================================================
# 2️⃣ HÀM LẤY NGÀY GẦN NHẤT TRONG DB
# ============================================================

def get_max_fiscal_date(symbol,engine):
    try:
        with engine.connect() as connection:
            query = text("SELECT MAX(fiscal_date_ending) FROM alphavantage_cash_flow WHERE symbol = :symbol")
            result = connection.execute(query, {"symbol": symbol}).scalar()
            return pd.to_datetime(result) if result else None
    except Exception as e:
        print(f"Lỗi khi truy vẫn ngày gần nhất cho mã {symbol}: {e}")
        return None
    
# ============================================================
# 3️⃣ GỌI API ALPHAVANTAGE
# ============================================================

def fetch_cashflow_from_api(symbol, api_key):
    url = f"https://www.alphavantage.co/query?function=CASH_FLOW&symbol={symbol}&apikey={api_key}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()  # Lỗi cho HTTP status codes 4xx/5xx

        # Thử giải mã JSON
        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"[{symbol}] Lỗi: Không thể giải mã phản hồi JSON từ API.")
            print(f"Nội dung phản hồi: {response.text}")
            return {"annual": [], "quarterly": []}
        
        # --- SỬA ĐỔI: Xử lý các thông báo giới hạn API ("Note") ---
        if "Note" in data:
            print(f"[{symbol}] ⚠️ API Limit Reached (Note): {data['Note']}")
            return {"annual": [], "quarterly": []}
            
        # Kiểm tra các thông báo lỗi khác
        if "Error Message" in data:
            print(f"[{symbol}] Lỗi từ API: {data['Error Message']}")
            return {"annual": [], "quarterly": []}
        if "Information" in data:
            print(f"[{symbol}] Thông tin từ API: {data['Information']}")
            return {"annual": [], "quarterly": []}
        
        # Kiểm tra dữ liệu báo cáo
        annual_reports = data.get("annualReports", [])
        quarterly_reports = data.get("quarterlyReports", [])

        if not annual_reports and not quarterly_reports:
            print(f"[{symbol}] Không tìm thấy báo cáo hàng năm hoặc hàng quý trong phản hồi API.")
            return {"annual": [], "quarterly": []}

        # Trả về key 'annual' và 'quarterly' để khớp với hàm main
        return {"annual": annual_reports, "quarterly": quarterly_reports} 

    except requests.exceptions.Timeout:
        print(f"[{symbol}] Lỗi: Hết thời gian chờ khi gọi API.")
        return {"annual": [], "quarterly": []}
    except requests.exceptions.RequestException as e:
        print(f"[{symbol}] Lỗi kết nối đến API: {e}")
        return {"annual": [], "quarterly": []}
    except Exception as e:
        print(f"[{symbol}] Lỗi không xác định đã xảy ra: {e}")
        return {"annual": [], "quarterly": []}
    
# ============================================================
# 4️⃣ CHUYỂN ĐỔI DỮ LIỆU
# ============================================================

def transform_cashflow_data(raw_data, symbol, report_type):
    if not raw_data:
        return pd.DataFrame()
    
    df = pd.DataFrame(raw_data)
    df['symbol'] = symbol
    df['report_type'] = report_type
    
    # Thay thế chuỗi 'nan' bằng giá trị NaN của numpy
    # ĐÃ XÓA DÒNG GÂY LỖI: pd.set_option('future.no_silent_downcasting', True)
    df.replace("nan", np.nan, inplace=True)
    
    # Loại cột 'report_type' ra khỏi danh sách cột số
    numeric_cols = df.columns.difference(['fiscalDateEnding', 'reportedCurrency', 'symbol', 'report_type'])
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    # Thay thế NaN bằng None để tương thích với DB
    df = df.replace({np.nan: None})
    
    column_mapping = {
        'fiscalDateEnding': 'fiscal_date_ending',
        'reportedCurrency': 'reported_currency',
        'operatingCashflow': 'operating_cashflow',
        'paymentsForOperatingActivities': 'payments_for_operating_activities',
        'proceedsFromOperatingActivities': 'proceeds_from_operating_activities',
        'changeInOperatingLiabilities': 'change_in_operating_liabilities',
        'changeInOperatingAssets': 'change_in_operating_assets',
        'depreciationDepletionAndAmortization': 'depreciation_depletion_and_amortization',
        'changeInReceivables': 'change_in_receivables',
        'changeInInventory': 'change_in_inventory',
        'capitalExpenditures': 'capital_expenditures',
        'cashflowFromInvestment': 'cashflow_from_investment',
        'cashflowFromFinancing': 'cashflow_from_financing',
        'proceedsFromRepaymentsOfShortTermDebt': 'proceeds_from_repayments_of_short_term_debt',
        'paymentsForRepurchaseOfCommonStock': 'payments_for_repurchase_of_common_stock',
        'paymentsForRepurchaseOfEquity': 'payments_for_repurchase_of_equity',
        'paymentsForRepurchaseOfPreferredStock': 'payments_for_repurchase_of_preferred_stock',
        'dividendPayout': 'dividend_payout',
        'dividendPayoutCommonStock': 'dividend_payout_common_stock',
        'dividendPayoutPreferredStock': 'dividend_payout_preferred_stock',
        'proceedsFromIssuanceOfCommonStock': 'proceeds_from_issuance_of_common_stock',
        'proceedsFromIssuanceOfLongTermDebtAndCapitalSecurities': 'proceeds_from_issuance_of_long_term',
        'proceedsFromIssuanceOfPreferredStock': 'proceeds_from_issuance_of_preferred_stock',
        'proceedsFromRepurchaseOfEquity': 'proceeds_from_repurchase_of_equity',
        'proceedsFromSalesOfTreasuryStock': 'proceeds_from_sale_of_treasury_stock',
        'changeInCashAndCashEquivalents': 'change_in_cash_and_cash_equivalents',
        'changeInExchangeRate': 'change_in_exchange_rate',
        'netIncome': 'net_income'
    }
    
    df.rename(columns=column_mapping, inplace=True)
    df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])
    
    # Xóa các bản ghi trùng lặp dựa trên fiscal_date_ending và report_type
    df.drop_duplicates(subset=['fiscal_date_ending', 'report_type'], keep='first', inplace=True)
    
    ordered_cols = [
        "symbol", "fiscal_date_ending", "reported_currency", "report_type",
        "operating_cashflow",
        "payments_for_operating_activities",
        "proceeds_from_operating_activities",
        "change_in_operating_liabilities",
        "change_in_operating_assets",
        "depreciation_depletion_and_amortization",
        "change_in_receivables",
        "change_in_inventory",
        "capital_expenditures",
        "cashflow_from_investment",
        "cashflow_from_financing",
        "proceeds_from_repayments_of_short_term_debt",
        "payments_for_repurchase_of_common_stock",
        "payments_for_repurchase_of_equity",
        "payments_for_repurchase_of_preferred_stock",
        "dividend_payout",
        "dividend_payout_common_stock",
        "dividend_payout_preferred_stock",
        "proceeds_from_issuance_of_common_stock",
        "proceeds_from_issuance_of_long_term",
        "proceeds_from_issuance_of_preferred_stock",
        "proceeds_from_repurchase_of_equity",
        "proceeds_from_sale_of_treasury_stock",
        "change_in_cash_and_cash_equivalents",
        "change_in_exchange_rate",
        "net_income",
    ]
    df = df[[c for c in ordered_cols if c in df.columns]]
    return df
# ============================================================
# 5️⃣ GHI DỮ LIỆU VÀO DATABASE
# ============================================================
def load_cashflow_to_db(engine, df, symbol):
    print(f"[{symbol}] Chuẩn bị ghi {len(df)} bản ghi vào DB.", flush=True)
    if df.empty:
        print(f"[{symbol}] Không có dữ liệu để ghi.", flush=True)
        return
    
    from sqlalchemy.dialects.mysql import insert as mysql_insert

    with engine.connect() as connection:
        metadata = MetaData()
        table = Table('alphavantage_cash_flow', metadata, autoload_with=engine)
        data_to_insert = df.to_dict(orient='records')

        insert_stmt = mysql_insert(table).values(data_to_insert)

        update_columns = {
            col: insert_stmt.inserted[col] for col in df.columns if col not in ['symbol', 'fiscal_date_ending', 'report_type', 'id']
        }

        upsert_stmt = insert_stmt.on_duplicate_key_update(**update_columns)
        
        transaction = connection.begin()
        try:
            connection.execute(upsert_stmt)
            transaction.commit()
            
            query = text("SELECT COUNT(*) FROM alphavantage_cash_flow WHERE symbol = :symbol")
            count = connection.execute(query, {"symbol": symbol}).scalar()
            print(f"[{symbol}] ✅ Đã ghi thành công và/hoặc cập nhật {len(data_to_insert)} bản ghi. Tổng số bản ghi cho mã này: {count}.", flush=True)
            
        except Exception as e:
            print(f"[{symbol}] ❌ Lỗi khi ghi dữ liệu vào DB: {e}", flush=True)
            transaction.rollback()
            
# ============================================================
# 6️⃣ HÀM MAIN
# ============================================================
def fetch_cashflow_data(mode='daily'):
    load_dotenv(".env")
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not api_key:
        raise ValueError("API key cho AlphaVantage không được tìm thấy trong file .env")
    start_date_historical = os.getenv("START_DATE", "2015-01-01")
    start_date_historical = pd.to_datetime(start_date_historical)
    
    engine = get_db_engine()
    stock_list_path = os.path.join(os.path.dirname(__file__), '..', 'stock_list.csv')
    try: 
        tickers_df = pd.read_csv(stock_list_path)
        tickers = tickers_df['symbol'].tolist()
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file stock_list.csv tại '{stock_list_path}'")
        return
    
    print(f"Bắt đầu fetch dữ liệu lưu chuyển tiền tệ cho các mã: {tickers}")
    
    for symbol in tickers:
        print(f"=== [{symbol}] BẮT ĐẦU ===")
        raw_data = fetch_cashflow_from_api(symbol, api_key)
        
        # SỬA ĐỔI: Dùng .get() để tránh KeyError khi bị limit API
        if not raw_data.get('annual') and not raw_data.get('quarterly'):
            print(f"[{symbol}] Không có dữ liệu báo cáo lưu chuyển tiền tệ hoặc bị Limit API. Bỏ qua.")
            continue
        
        max_date = get_max_fiscal_date(symbol, engine)
        annual_df = transform_cashflow_data(raw_data["annual"], symbol, "annual")
        quarterly_df = transform_cashflow_data(raw_data["quarterly"], symbol, "quarterly")
        combined_df = pd.concat([annual_df, quarterly_df], ignore_index=True)
        # loại bỏ các hàng không có fiscal_date_ending
        combined_df = combined_df[combined_df["fiscal_date_ending"].notna()]
        
        # APPLY FILTER THEO MODE
        if mode == "historical":
            # giữ các báo cáo từ START_DATE đến hiện tại
            combined_df = combined_df[combined_df["fiscal_date_ending"] >= start_date_historical]
            print(f"[{symbol}] historical: giữ báo cáo từ {start_date_historical} trở về sau. Số bản ghi sau lọc: {len(combined_df)}")
        else:  # mode == "daily"
            if max_date is not None:
                # max_date là pd.Timestamp — so sánh với date
                combined_df = combined_df[combined_df["fiscal_date_ending"] > max_date]
                print(f"[{symbol}] daily: giữ báo cáo mới hơn {max_date.date()}. Số bản ghi sau lọc: {len(combined_df)}")

        if combined_df.empty:
            print(f"[{symbol}] ❗ Sau khi lọc không còn bản ghi để ghi. Bỏ qua.")
        else:
            load_cashflow_to_db(engine, combined_df, symbol)

        
        sleep(5)

    print("\n✅ Hoàn tất quá trình fetch cash flow.")
    
# ============================================================
# 7️⃣ CHẠY TỪ DÒNG LỆNH
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch cash flow data from Alpha Vantage API.")
    parser.add_argument(
        "--mode",
        type=str,
        default="daily",
        choices=["daily", "historical"],
        help="Chế độ fetch: 'daily' chỉ lấy dữ liệu mới hơn trong DB, 'historical' lấy toàn bộ lịch sử."
    )
    args = parser.parse_args()

    fetch_cashflow_data(mode=args.mode)