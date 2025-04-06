from db_connect import connect_db  # Import connect_db từ file db_connect.py
from utils_fundamental import get_all_tickers, parse_quarterly_data, check_and_insert_dim_time
from datetime import datetime

START_DATE = datetime(2020, 1, 1)
CURRENT_DATE = datetime.now()

def close_db_connection(cursor, conn):
    """Đóng con trỏ và kết nối cơ sở dữ liệu"""
    if cursor:
        cursor.close()
    if conn and conn.is_connected():
        conn.close()

def update_fundamental_for_all_stocks():
    tickers = get_all_tickers()
    
    for ticker in tickers:
        print(f"➡️ Đang lấy dữ liệu fundamental cho {ticker}")
        quarter_data = parse_quarterly_data(ticker, START_DATE, CURRENT_DATE)
        
        if quarter_data:
            update_fact_fundamental_batch(quarter_data)
            print(f"✅ Đã cập nhật dữ liệu fundamental cho {ticker}")
        else:
            print(f"⚠️ Không có dữ liệu fundamental cho {ticker}")

def update_fact_fundamental_batch(quarter_data):
    if not quarter_data:
        return

    conn = connect_db()
    if not conn:
        print("❌ Không thể kết nối cơ sở dữ liệu trong update_fact_fundamental_batch")
        return

    cursor = conn.cursor()

    insert_query = """
    INSERT INTO fact_company_financials_quarterly 
    (stock_id, time_id, fiscal_year, fiscal_quarter, revenue, net_income,
     total_assets, total_liabilities, operating_cash_flow, eps, currency)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for item in quarter_data:
        stock_id = item['stock_id']
        time_id = item['time_id']

        # Check time_id & stock_id
        cursor.execute("""
            SELECT 1 FROM fact_company_financials_quarterly 
            WHERE stock_id = %s AND time_id = %s
        """, (stock_id, time_id))
        exists = cursor.fetchone()

        if exists:
            print(f"⚠️ Dữ liệu đã tồn tại cho stock_id={stock_id}, time_id={time_id} ➔ Bỏ qua.")
            continue  # Skip insert nếu đã có

        # Nếu chưa có thì insert
        cursor.execute(insert_query, (
            stock_id,
            time_id,
            item['fiscal_year'],
            item['fiscal_quarter'],
            item.get('revenue'),
            item.get('net_income'),
            item.get('total_assets'),
            item.get('total_liabilities'),
            item.get('operating_cash_flow'),
            item.get('eps'),
            item.get('currency')
        ))

    conn.commit()
    close_db_connection(cursor, conn)
