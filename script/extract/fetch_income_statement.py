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
def get_db_engine():
    load_dotenv(".env")
    
    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_db = os.getenv("MYSQL_DATABASE")
    mysql_host = "localhost"
    mysql_port = "3307"
    
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
            query = text("SELECT MAX(fiscal_date_ending) FROM alphavantage_income_statement WHERE symbol = :symbol")
            result = connection.execute(query, {"symbol": symbol}).scalar()
            return pd.to_datetime(result) if result else None
    except Exception as e:
        print(f"Lỗi khi truy vẫn ngày gần nhất cho mã {symbol}: {e}")
        return None
    
# ============================================================ 
# 3️⃣ GỌI API ALPHAVANTAGE
# ============================================================ 

def fetch_income_statement_from_api(symbol, api_key):
    url = f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey={api_key}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Lỗi cho HTTP status codes 4xx/5xx

        # Thử giải mã JSON
        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"[{symbol}] Lỗi: Không thể giải mã phản hồi JSON từ API.")
            print(f"Nội dung phản hồi: {response.text}")
            return {"annualReports": [], "quarterlyReports": []}

        # Kiểm tra xem API có trả về thông báo lỗi không
        if "Error Message" in data:
            print(f"[{symbol}] Lỗi từ API: {data['Error Message']}")
            return {"annualReports": [], "quarterlyReports": []}
        if "Information" in data:
            print(f"[{symbol}] Thông tin từ API: {data['Information']}")
            return {"annualReports": [], "quarterlyReports": []}
        
        # Kiểm tra dữ liệu báo cáo
        annual_reports = data.get("annualReports", [])
        quarterly_reports = data.get("quarterlyReports", [])

        if not annual_reports and not quarterly_reports:
            print(f"[{symbol}] Không tìm thấy báo cáo hàng năm hoặc hàng quý trong phản hồi API.")
            print(f"Phản hồi đầy đủ: {data}")
            return {"annualReports": [], "quarterlyReports": []}

        return {"annual": annual_reports, "quarterly": quarterly_reports}

    except requests.exceptions.Timeout:
        print(f"[{symbol}] Lỗi: Hết thời gian chờ khi gọi API.")
        return {"annualReports": [], "quarterlyReports": []}
    except requests.exceptions.RequestException as e:
        print(f"[{symbol}] Lỗi kết nối đến API: {e}")
        return {"annualReports": [], "quarterlyReports": []}
    except Exception as e:
        print(f"[{symbol}] Lỗi không xác định đã xảy ra: {e}")
        return {"annualReports": [], "quarterlyReports": []}
    
# ============================================================ 
# 4️⃣ CHUYỂN ĐỔI DỮ LIỆU
# ============================================================ 

def transform_income_statement_data(raw_data, symbol, report_type):
    if not raw_data:
        return pd.DataFrame()
    
    df = pd.DataFrame(raw_data)
    df['symbol'] = symbol
    df['report_type'] = report_type
    
    df.replace("None", np.nan, inplace=True)
    df.replace("nan", np.nan, inplace=True)
    
    numeric_cols = df.columns.difference(['fiscalDateEnding', 'reportedCurrency', 'symbol', 'report_type'])
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    df = df.replace({np.nan: None})
    
    column_mapping = {
        'fiscalDateEnding': 'fiscal_date_ending',
        'reportedCurrency': 'reported_currency',
        'totalRevenue': 'total_revenue',
        'grossProfit': 'gross_profit',
        'costOfRevenue': 'cost_of_revenue',
        'costofGoodsAndServicesSold': 'cost_of_goods_and_services_sold',
        'operatingIncome': 'operating_income',
        'sellingGeneralAndAdministrative': 'selling_general_and_administrative',
        'researchAndDevelopment': 'research_and_development',
        'operatingExpenses': 'operating_expenses',
        'investmentIncomeNet': 'investment_income_net',
        'netInterestIncome': 'net_interest_income',
        'interestIncome': 'interest_income',
        'interestExpense': 'interest_expense',
        'nonInterestIncome': 'non_interest_income',
        'otherNonOperatingIncome': 'other_non_operating_income',
        'depreciation': 'depreciation',
        'depreciationAndAmortization': 'depreciation_and_amortization',
        'incomeBeforeTax': 'income_before_tax',
        'incomeTaxExpense': 'income_tax_expense',
        'interestAndDebtExpense': 'interest_and_debt_expense',
        'netIncomeFromContinuingOperations': 'net_income_from_continuing_operations',
        'comprehensiveIncomeNetOfTax': 'comprehensive_income_net_of_tax',
        'ebit': 'ebit',
        'ebitda': 'ebitda',
        'netIncome': 'net_income'
    }
    
    df.rename(columns=column_mapping, inplace=True)
    df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])
    
    df.drop_duplicates(subset=['fiscal_date_ending', 'report_type'], keep='first', inplace=True)
    
    ordered_cols = list(column_mapping.values()) + ['symbol', 'report_type']
    df = df[[c for c in ordered_cols if c in df.columns]]
    return df

# ============================================================ 
# 5️⃣ GHI DỮ LIỆU VÀO DATABASE
# ============================================================ 
def load_income_statement_to_db(engine, df, symbol):
    print(f"[{symbol}] Chuẩn bị ghi {len(df)} bản ghi vào DB.", flush=True)
    if df.empty:
        print(f"[{symbol}] Không có dữ liệu để ghi.", flush=True)
        return
    
    from sqlalchemy.dialects.mysql import insert as mysql_insert

    with engine.connect() as connection:
        metadata = MetaData()
        table = Table('alphavantage_income_statement', metadata, autoload_with=engine)
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
            
            query = text("SELECT COUNT(*) FROM alphavantage_income_statement WHERE symbol = :symbol")
            count = connection.execute(query, {"symbol": symbol}).scalar()
            print(f"[{symbol}] ✅ Đã ghi thành công và/hoặc cập nhật {len(data_to_insert)} bản ghi. Tổng số bản ghi cho mã này: {count}.", flush=True)
            
        except Exception as e:
            print(f"[{symbol}] ❌ Lỗi khi ghi dữ liệu vào DB: {e}", flush=True)
            transaction.rollback()
            
# ============================================================ 
# 6️⃣ HÀM MAIN
# ============================================================ 
def fetch_income_statement_data(mode='daily'):
    load_dotenv(".env")
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not api_key:
        raise ValueError("API key cho AlphaVantage không được tìm thấy trong file .env")
    start_date_historical = os.getenv("START_DATE", "2015-01-01")
    start_date_historical = pd.to_datetime(start_date_historical)
    
    engine = get_db_engine()
    stock_list_path = os.path.join(os.path.dirname(__file__), "..", "..", "stock_list.csv")
    try: 
        tickers_df = pd.read_csv(stock_list_path)
        tickers = tickers_df['symbol'].tolist()
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file stock_list.csv tại '{stock_list_path}'")
        return
    
    print(f"Bắt đầu fetch dữ liệu income statement cho các mã: {tickers}")
    
    for symbol in tickers:
        print(f"=== [{symbol}] BẮT ĐẦU ===")
        raw_data = fetch_income_statement_from_api(symbol, api_key)
        if not raw_data['annual'] and not raw_data['quarterly']:
            print(f"[{symbol}] Không có dữ liệu income statement. Bỏ qua.")
            continue
        
        max_date = get_max_fiscal_date(symbol, engine)
        annual_df = transform_income_statement_data(raw_data["annual"], symbol, "annual")
        quarterly_df = transform_income_statement_data(raw_data["quarterly"], symbol, "quarterly")
        combined_df = pd.concat([annual_df, quarterly_df], ignore_index=True)
        combined_df = combined_df[combined_df["fiscal_date_ending"].notna()]
        
        if mode == "historical":
            combined_df = combined_df[combined_df["fiscal_date_ending"] >= start_date_historical]
            print(f"[{symbol}] historical: giữ báo cáo từ {start_date_historical} trở về sau. Số bản ghi sau lọc: {len(combined_df)}")
        else:  # mode == "daily"
            if max_date is not None:
                combined_df = combined_df[combined_df["fiscal_date_ending"] > max_date]
                print(f"[{symbol}] daily: giữ báo cáo mới hơn {max_date.date()}. Số bản ghi sau lọc: {len(combined_df)}")

        if combined_df.empty:
            print(f"[{symbol}] ❗ Sau khi lọc không còn bản ghi để ghi. Bỏ qua.")
        else:
            load_income_statement_to_db(engine, combined_df, symbol)

        sleep(15)

    print("\n✅ Hoàn tất quá trình fetch income statement.")
    
# ============================================================ 
# 7️⃣ CHẠY TỪ DÒNG LỆNH
# ============================================================ 
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch income statement data from Alpha Vantage API.")
    parser.add_argument(
        "--mode",
        type=str,
        default="daily",
        choices=["daily", "historical"],
        help="Chế độ fetch: 'daily' chỉ lấy dữ liệu mới hơn trong DB, 'historical' lấy toàn bộ lịch sử."
    )
    args = parser.parse_args()

    fetch_income_statement_data(mode=args.mode)
