import requests
from db_connect import connect_db
from config import API_KEY_APV
from datetime import datetime

def get_db_connection():
    conn = connect_db()
    if not conn:
        print("❌ Không thể kết nối cơ sở dữ liệu trong get_db_connection")
    return conn

def close_db_connection(cursor, conn):
    if cursor:
        cursor.close()
    if conn and conn.is_connected():
        conn.close()

def get_all_tickers():
    conn = get_db_connection()
    if not conn:
        return []
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT ticker FROM dim_stock;")
        tickers = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"❌ Lỗi khi lấy danh sách ticker: {e}")
        tickers = []
    finally:
        close_db_connection(cursor, conn)
    return tickers

def get_stock_id_by_ticker(ticker):
    conn = get_db_connection()
    if not conn:
        return None
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT stock_id FROM dim_stock WHERE ticker = %s", (ticker,))
        stock_id = cursor.fetchone()
        return stock_id[0] if stock_id else None
    except Exception as e:
        print(f"❌ Lỗi khi lấy stock_id cho {ticker}: {e}")
        return None
    finally:
        close_db_connection(cursor, conn)

def fetch_alpha_vantage_data(ticker, function):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": function,
        "symbol": ticker,
        "apikey": API_KEY_APV
    }
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        return None

def check_and_insert_dim_time(time_id, conn=None, cursor=None):
    """Kiểm tra xem time_id có tồn tại trong dim_time không"""
    close_conn = False
    if conn is None or cursor is None:
        conn = get_db_connection()
        if not conn:
            return False
        cursor = conn.cursor()
        close_conn = True
    
    try:
        cursor.execute("SELECT 1 FROM dim_time WHERE date_key = %s", (time_id,))
        result = cursor.fetchone()
        
        if not result:
            if close_conn:
                close_db_connection(cursor, conn)
            return False
    except Exception as e:
        print(f"❌ Lỗi khi kiểm tra time_id {time_id}: {e}")
        if close_conn:
            close_db_connection(cursor, conn)
        return False
    
    if close_conn:
        close_db_connection(cursor, conn)
    return True

def parse_quarterly_data(ticker, start_date, end_date):
    income_data = fetch_alpha_vantage_data(ticker, "INCOME_STATEMENT")
    balance_data = fetch_alpha_vantage_data(ticker, "BALANCE_SHEET")
    cashflow_data = fetch_alpha_vantage_data(ticker, "CASH_FLOW")
    earnings_data = fetch_alpha_vantage_data(ticker, "EARNINGS")
    
    if not (income_data and balance_data and cashflow_data and earnings_data):
        return []

    income_quarters = income_data.get("quarterlyReports", [])
    balance_quarters = balance_data.get("quarterlyReports", [])
    cashflow_quarters = cashflow_data.get("quarterlyReports", [])
    earnings_quarters = earnings_data.get("quarterlyEarnings", [])

    stock_id = get_stock_id_by_ticker(ticker)
    if stock_id is None:
        return []
    
    quarter_data = []
    
    conn = get_db_connection()
    if not conn:
        return []
    cursor = conn.cursor()

    for i in range(min(len(income_quarters), len(balance_quarters), len(cashflow_quarters), len(earnings_quarters))):
        income = income_quarters[i]
        balance = balance_quarters[i]
        cashflow = cashflow_quarters[i]
        earnings = earnings_quarters[i]

        fiscal_date_ending = income.get("fiscalDateEnding")
        if not fiscal_date_ending:
            continue
        
        try:
            fiscal_date_obj = datetime.strptime(fiscal_date_ending, "%Y-%m-%d")
        except ValueError:
            continue
        
        # Kiểm tra nếu fiscal_date_ending nằm trong khoảng từ start_date đến end_date
        if start_date <= fiscal_date_obj <= end_date:
            fiscal_year = int(fiscal_date_ending[:4])
            fiscal_month = int(fiscal_date_ending[5:7])
            
            # Tính fiscal_quarter từ tháng
            if 1 <= fiscal_month <= 3:
                fiscal_quarter = 1
            elif 4 <= fiscal_month <= 6:
                fiscal_quarter = 2
            elif 7 <= fiscal_month <= 9:
                fiscal_quarter = 3
            else:
                fiscal_quarter = 4

            time_id = int(fiscal_date_ending.replace("-", ""))  # YYYYMMDD
            
            # Kiểm tra time_id trong dim_time
            if not check_and_insert_dim_time(time_id, conn, cursor):
                continue

            item = {
                "stock_id": stock_id,
                "time_id": time_id,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter,
                "revenue": income.get("totalRevenue"),
                "net_income": income.get("netIncome"),
                "total_assets": balance.get("totalAssets"),
                "total_liabilities": balance.get("totalLiabilities"),
                "operating_cash_flow": cashflow.get("operatingCashflow"),
                "eps": earnings.get("reportedEPS"),
                "currency": income.get("reportedCurrency")
            }
            quarter_data.append(item)
    
    close_db_connection(cursor, conn)

    return quarter_data