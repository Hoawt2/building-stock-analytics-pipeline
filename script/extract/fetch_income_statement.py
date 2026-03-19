import os
import argparse
import requests
import pandas as pd     
import numpy as np 
from dotenv import load_dotenv 
from datetime import datetime 
from time import sleep 

# Cấu hình thư mục lưu Parquet 
PARQUET_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw_parquet')
FILE_NAME = 'alphavantage_income_statement.parquet'
FILE_PATH = os.path.join(PARQUET_DIR, FILE_NAME)

def get_max_fiscal_date(symbol): 
    """
    Lấy ngày báo cáo gần nhất từ file Parquet
    """
    if os.path.exists(FILE_PATH):
        try: 
            df = pd.read_parquet(FILE_PATH, columns=['symbol', 'fiscal_date_ending'])
            df_symbol = df[df['symbol'] == symbol]
            
            if not df_symbol.empty: 
                max_date = df_symbol['fiscal_date_ending'].max()
                return pd.to_datetime(max_date)
        except Exception as e: 
            print(f'Lỗi khi đọc file Parquet để tìm max date cho {symbol}: {e}')
    return None

# Gọi API 
def fetch_income_statement_from_api(symbol, api_key):
    url = f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey={api_key}"
    try: 
        response = requests.get(url, timeout=5)
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
def transform_income_statement_data(raw_data, symbol, report_type): 
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
    if 'fiscal_date_ending' in df.columns:
        df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])
        df.drop_duplicates(subset=['fiscal_date_ending', 'report_type'], keep='first', inplace=True)
        
    ordered_cols = list(column_mapping.values()) + ['symbol', 'report_type']
    df = df[[c for c in ordered_cols if c in df.columns]]
    return df

# Lưu dữ liệu vào PARQUET
def fetch_income_statement_data(mode='daily'):
    load_dotenv(".env")
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    start_date_historical = pd.to_datetime(os.getenv("START_DATE", "2015-01-01"))
    
    os.makedirs(PARQUET_DIR, exist_ok=True)
    stock_list_path = os.path.join(os.path.dirname(__file__), '..', 'stock_list.csv')

    try: 
        tickers_df = pd.read_csv(stock_list_path)
        tickers = tickers_df['symbol'].tolist()
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file {stock_list_path}")
        return
    
    print(f"Bắt đầu fetch dữ liệu Income Statement cho: {tickers}")
    
    all_new_data = [] # Chứa các DataFrame báo cáo mới thu thập được
    for symbol in tickers:
        print(f"--- [{symbol}] ---")
        raw_data = fetch_income_statement_from_api(symbol, api_key)
        
        if not raw_data.get('annual') and not raw_data.get('quarterly'):
            print(f"[{symbol}] Rỗng hoặc Limit API. Bỏ qua.")
            sleep(2) # Tránh spam API dồn dập
            continue
    
        max_date = get_max_fiscal_date(symbol)
        
        annual_df = transform_income_statement_data(raw_data.get("annual", []), symbol, "annual")
        quarterly_df = transform_income_statement_data(raw_data.get("quarterly", []), symbol, "quarterly")
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
        
        if mode == 'daily' and os.path.exists(FILE_PATH):
            print("Đang gộp báo cáo tài chính mới vào file Parquet cũ...")
            old_df = pd.read_parquet(FILE_PATH)
            final_df = pd.concat([old_df, new_df], ignore_index=True)
            # Khử trùng lặp trên bộ khoá (ngày báo cáo + mã + loại)
            final_df.drop_duplicates(subset=['symbol', 'fiscal_date_ending', 'report_type'], keep='last', inplace=True)
        else:
            final_df = new_df
        
        final_df['load_timestamp'] = pd.Timestamp.now()
        final_df.to_parquet(FILE_PATH, index=False)
        print(f"Hoàn tất! Đã lưu tổng cộng {len(final_df)} báo cáo vào Data Lake (Parquet).")
    else:
        print("Không có dữ liệu Income Statement mới nào trên toàn thị trường cần tải.")
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, default="daily", choices=["daily", "historical"])
    args = parser.parse_args()
    fetch_income_statement_data(mode=args.mode)