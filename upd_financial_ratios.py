from utils_ratio_quarter import fetch_fmp_key_metrics, get_stock_list, get_time_id, check_existing_record, insert_financial_ratios
import datetime
from config import API_KEY_FMP

def update_financial_ratios():
    print("[DEBUG] Starting update_financial_ratios")
    stocks = get_stock_list()
    if not stocks:
        print("[ERROR] No stocks retrieved, aborting")
        return
        
    start_year = 2020
    today = datetime.date.today()
    current_year = today.year

    print(f"[DEBUG] Processing {len(stocks)} stocks from {start_year} to {current_year}")
    for stock in stocks:
        ticker = stock['ticker']
        stock_id = stock['stock_id']
        print(f"[DEBUG] Processing ticker: {ticker}, stock_id: {stock_id}")

        metrics_data = fetch_fmp_key_metrics(ticker, API_KEY_FMP)
        
        if not metrics_data:
            print(f"[WARNING] No metrics data for {ticker}")
            continue

        for year_data in metrics_data:
            date_str = year_data.get('date')
            if not date_str:
                print(f"[WARNING] No date found in data for {ticker}")
                continue
                
            try:
                date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError as e:
                print(f"[ERROR] Invalid date format for {ticker}: {date_str}, error: {e}")
                continue
                
            year = date.year
            
            if year < start_year or year > current_year:
                continue

            print(f"[DEBUG] Processing data for {ticker} on {date}")

            time_id = get_time_id(date)
            if not time_id:
                print(f"[ERROR] No time_id found for date {date}")
                continue
            print(f"[DEBUG] time_id for {date}: {time_id}")

            if check_existing_record(stock_id, time_id):
                print(f"[DEBUG] Record already exists for stock_id={stock_id}, time_id={time_id}, skipping")
                continue

            record = {
                'stock_id': stock_id,
                'time_id': time_id,
                'current_ratio': round(float(year_data.get('currentRatio', 0)), 2),
                'roe': round(float(year_data.get('roe', 0)), 2),
                'debt_to_equity': round(float(year_data.get('debtToEquity', 0)), 2),
                'pe_ratio': round(float(year_data.get('peRatio', 0)), 2),
                'pb_ratio': round(float(year_data.get('pbRatio', 0)), 2),
                'dividend_yield': round(float(year_data.get('dividendYield', 0)), 2),
                'interest_coverage_ratio': round(float(year_data.get('interestCoverage', 0)), 2)
            }
            print(f"[DEBUG] Prepared record for {ticker}: {record}")

            insert_financial_ratios(record)
            print(f"[INFO] Inserted financial ratios for {ticker} on {date}")
