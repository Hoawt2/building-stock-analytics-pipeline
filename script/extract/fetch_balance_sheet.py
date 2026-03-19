import os
import argparse
import requests
import pandas as pd     
import numpy as np 
from dotenv import load_dotenv 
from datetime import datetime 
from time import sleep 

# Cấu hình kết nối MINIO 
load_dotenv()
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', "http://localhost:9000")  # Nếu chạy trong docker thì là http://minio:9000
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', "minioadmin")
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', "minioadmin")
BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME', "stock-data")
# Đường dẫn ảo S3 tới file Parquet
S3_FILE_PATH = f"s3://{BUCKET_NAME}/raw_parquet/alphavantage_balance_sheet.parquet"
# Cấu hình "chìa khoá" để Pandas biết đường mở cửa MinIO
STORAGE_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
}

def get_max_fiscal_date(symbol): 
    """
    Lấy ngày báo cáo gần nhất từ file Parquet
    """
    try: 
        df = pd.read_parquet(S3_FILE_PATH, columns=['symbol', 'fiscal_date_ending'], storage_options=STORAGE_OPTIONS)
        df_symbol = df[df['symbol'] == symbol]
        
        if not df_symbol.empty: 
            max_date = df_symbol['fiscal_date_ending'].max()
            return pd.to_datetime(max_date)
    except Exception as e: 
        print(f'Lỗi khi đọc file Parquet để tìm max date cho {symbol}: {e}')
    return None

# Gọi API 
def fetch_balance_sheet_from_api(symbol, api_key):
    url = f"https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={symbol}&apikey={api_key}"
    try: 
        response = requests.get(url, timeout=15)
        response.raise_for_status()

        try: 
            data = response.json()
        except requests.exceptions.JSONDecodeError: 
            print(f"[{symbol}] Lỗi không thể giải mã JSON.")
            return {"annual": [], "quarterly": []}
        
        if "Note" in data: 
            print(f"[{symbol}]  Limit API: {data['Note']}")
            return {"annual": [], "quarterly": []}
        if "Error Message" in data:
            return {"annual": [], "quarterly": []}
        if "Information" in data:
            return {"annual": [], "quarterly": []}
        
        annual_reports = data.get("annualReports", [])
        quarterly_reports = data.get("quarterlyReports", [])

        return {"annual": annual_reports, "quarterly": quarterly_reports}
    
    except Exception as e: 
        print(f"[{symbol}] Lỗi khi gọi API: {e}")
        return {"annual": [], "quarterly": []}

# Biến đổi dữ liệu 
def transform_balance_sheet_data(raw_data, symbol, report_type): 
    if not raw_data: 
        return pd.DataFrame()
    
    df = pd.DataFrame(raw_data)
    df['symbol'] = symbol
    df['report_type'] = report_type
    df.replace(["None", "nan"], None)
    numeric_cols = df.columns.difference(['fiscalDateEnding', 'reportedCurrency', 'symbol', 'report_type'])
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    column_mapping = {
        'fiscalDateEnding': 'fiscal_date_ending',
        'reportedCurrency': 'reported_currency',
        'totalAssets': 'total_assets',
        'totalCurrentAssets': 'total_current_assets',
        'cashAndCashEquivalentsAtCarryingValue': 'cash_and_cash_equivalents_at_carrying_value',
        'cashAndShortTermInvestments': 'cash_and_short_term_investments',
        'inventory': 'inventory',
        'currentNetReceivables': 'current_net_receivables',
        'totalNonCurrentAssets': 'total_non_current_assets',
        'propertyPlantEquipment': 'property_plant_equipment',
        'accumulatedDepreciationAmortizationPPE': 'accumulated_depreciation_amortization_ppe',
        'intangibleAssets': 'intangible_assets',
        'intangibleAssetsExcludingGoodwill': 'intangible_assets_excluding_goodwill',
        'goodwill': 'goodwill',
        'investments': 'investments',
        'longTermInvestments': 'long_term_investments',
        'shortTermInvestments': 'short_term_investments',
        'otherCurrentAssets': 'other_current_assets',
        'otherNonCurrentAssets': 'other_non_current_assets',
        'totalLiabilities': 'total_liabilities',
        'totalCurrentLiabilities': 'total_current_liabilities',
        'currentAccountsPayable': 'current_accounts_payable',
        'deferredRevenue': 'deferred_revenue',
        'currentDebt': 'current_debt',
        'shortTermDebt': 'short_term_debt',
        'totalNonCurrentLiabilities': 'total_non_current_liabilities',
        'capitalLeaseObligations': 'capital_lease_obligations',
        'longTermDebt': 'long_term_debt',
        'currentLongTermDebt': 'current_long_term_debt',
        'longTermDebtNoncurrent': 'long_term_debt_noncurrent',
        'shortLongTermDebtTotal': 'short_long_term_debt_total',
        'otherCurrentLiabilities': 'other_current_liabilities',
        'otherNonCurrentLiabilities': 'other_non_current_liabilities',
        'totalShareholderEquity': 'total_shareholder_equity',
        'treasuryStock': 'treasury_stock',
        'retainedEarnings': 'retained_earnings',
        'commonStock': 'common_stock',
        'commonStockSharesOutstanding': 'common_stock_shares_outstanding'
    }

    df.rename(columns=column_mapping, inplace=True)
    if 'fiscal_date_ending' in df.columns:
        df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])
        df.drop_duplicates(subset=['fiscal_date_ending', 'report_type'], keep='first', inplace=True)
        
    ordered_cols = list(column_mapping.values()) + ['symbol', 'report_type']
    df = df[[c for c in ordered_cols if c in df.columns]]
    return df

# Lưu dữ liệu vào PARQUET
def fetch_balance_sheet_data(mode='daily'):
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    start_date_historical = pd.to_datetime(os.getenv("START_DATE", "2015-01-01"))
    stock_list_path = os.path.join(os.path.dirname(__file__), '..', 'stock_list.csv')

    try: 
        tickers_df = pd.read_csv(stock_list_path)
        tickers = tickers_df['symbol'].tolist()
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file {stock_list_path}")
        return
    
    print(f"Bắt đầu fetch dữ liệu Balance Sheet cho: {tickers}")
    
    all_new_data = [] # Chứa các DataFrame báo cáo mới thu thập được
    for symbol in tickers:
        print(f"--- [{symbol}] ---")
        raw_data = fetch_balance_sheet_from_api(symbol, api_key)
        
        if not raw_data.get('annual') and not raw_data.get('quarterly'):
            print(f"[{symbol}] Rỗng hoặc Limit API. Bỏ qua.")
            sleep(2) # Tránh spam API dồn dập
            continue
    
        max_date = get_max_fiscal_date(symbol)
        
        annual_df = transform_balance_sheet_data(raw_data.get("annual", []), symbol, "annual")
        quarterly_df = transform_balance_sheet_data(raw_data.get("quarterly", []), symbol, "quarterly")
        combined_df = pd.concat([annual_df, quarterly_df], ignore_index=True)

        if not combined_df.empty and "fiscal_date_ending" in combined_df.columns:
            combined_df = combined_df[combined_df["fiscal_date_ending"].notna()]
            
            if mode == "historical":
                combined_df = combined_df[combined_df["fiscal_date_ending"] >= start_date_historical]
            elif mode == "daily" and max_date is not None:
                combined_df = combined_df[combined_df["fiscal_date_ending"] > max_date]
            if not combined_df.empty:
                print(f"[{symbol}] Có {len(combined_df)} báo cáo mới!")
                all_new_data.append(combined_df)
            else:
                print(f"[{symbol}] Dữ liệu đã up-to-date.")
                
            sleep(5) # AlphaVantage giới hạn 5 API calls / phút cho bản free
    # Xử lý nghiền tất cả vào file Parquet
    if all_new_data:
        new_df = pd.concat(all_new_data, ignore_index=True)
        
        try:
            print("Đang gộp báo cáo tài chính mới vào file Parquet cũ...")
            old_df = pd.read_parquet(S3_FILE_PATH, storage_options=STORAGE_OPTIONS)
            final_df = pd.concat([old_df, new_df], ignore_index=True)
            # Khử trùng lặp trên bộ khoá (ngày báo cáo + mã + loại)
            final_df.drop_duplicates(subset=['symbol', 'fiscal_date_ending', 'report_type'], keep='last', inplace=True)
        except Exception:
            print("Không thấy file cũ trên MinIO. Tạo file Parquet mới tinh!")
            final_df = new_df
        
        final_df['load_timestamp'] = pd.Timestamp.now()
        final_df.to_parquet(S3_FILE_PATH, index=False, storage_options=STORAGE_OPTIONS)
        print(f"Hoàn tất! Đã lưu tổng cộng {len(final_df)} báo cáo vào Data Lake (Parquet).")
    else:
        print("Không có dữ liệu Balance Sheet mới nào trên toàn thị trường cần tải.")
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="daily", choices=["daily", "historical"])
    args = parser.parse_args()
    fetch_balance_sheet_data(mode=args.mode)