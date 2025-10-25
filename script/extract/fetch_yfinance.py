
import os
import argparse
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text, Table, MetaData
from dotenv import load_dotenv
from datetime import datetime, timedelta

def get_db_engine():
    """
    Tải các biến môi trường và tạo một SQLAlchemy engine để kết nối tới MySQL.
    Ghi đè host và port cho môi trường test local.
    """
    load_dotenv('.env')

    # Lấy thông tin kết nối từ biến môi trường
    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_db = os.getenv("MYSQL_DATABASE")

    # Ghi đè host và port cho local test, kết nối qua port-mapping của Docker
    mysql_host = "localhost"
    mysql_port = "3307"

    if not all([mysql_user, mysql_password, mysql_db, mysql_host, mysql_port]):
        raise ValueError("Một hoặc nhiều biến môi trường của database chưa được thiết lập.")

    # Tạo chuỗi kết nối và engine
    connection_string = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}"
    engine = create_engine(connection_string)
    return engine

def get_max_date_for_stock(symbol, engine):
    """
    Truy vấn database để lấy ngày gần nhất của một mã cổ phiếu.
    """
    try:
        with engine.connect() as connection:
            query = text("SELECT MAX(date) FROM raw_yfinance WHERE symbol = :symbol")
            result = connection.execute(query, {"symbol": symbol}).scalar()
            return pd.to_datetime(result) if result else None
    except Exception as e:
        print(f"Lỗi khi truy vấn ngày gần nhất cho mã {symbol}: {e}")
        return None

def fetch_yfinance_data(mode='daily'):
    """
    Fetch dữ liệu từ yfinance và lưu vào database.

    :param mode: 'daily' hoặc 'historical'.
                 'daily': fetch từ ngày cuối cùng trong db + 1 ngày.
                 'historical': fetch từ ngày START_DATE trong file .env.
    """
    load_dotenv() 
    
    # Lấy START_DATE từ .env cho historical mode, nếu không có thì dùng '2015-01-01'
    start_date_historical = os.getenv("START_DATE", '2015-01-01')

    engine = get_db_engine()
    
    # Đường dẫn tới file stock_list.csv
    # Giả định script được chạy từ gốc của project
    stock_list_path = os.path.join(os.path.dirname(__file__), '..', '..', 'stock_list.csv')
    
    try:
        tickers_df = pd.read_csv(stock_list_path)
        tickers = tickers_df['symbol'].tolist()
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file stock_list.csv tại '{stock_list_path}'")
        return

    print(f"Bắt đầu fetch dữ liệu cho các mã: {tickers}")

    for ticker in tickers:
        start_date = None
        if mode == 'daily':
            max_date = get_max_date_for_stock(ticker, engine)
            if max_date:
                start_date = (max_date + timedelta(days=1)).strftime('%Y-%m-%d')
                print(f"[{ticker}] Chế độ daily. Ngày cuối trong DB: {max_date.strftime('%Y-%m-%d')}. Fetch từ: {start_date}")
            else:
                # Nếu mã mới, fetch toàn bộ lịch sử
                start_date = start_date_historical
                print(f"[{ticker}] Mã mới. Fetch toàn bộ lịch sử từ: {start_date}")
        elif mode == 'historical':
            start_date = start_date_historical
            print(f"[{ticker}] Chế độ historical. Fetch từ: {start_date}")

        if not start_date:
            print(f"[{ticker}] Bỏ qua vì không xác định được ngày bắt đầu.")
            continue
            
        end_date = datetime.now().strftime('%Y-%m-%d')

        # Chỉ fetch nếu ngày bắt đầu nhỏ hơn hoặc bằng ngày kết thúc
        if start_date > end_date:
            print(f"[{ticker}] Dữ liệu đã được cập nhật. Bỏ qua.")
            continue

        try:
            # Tải dữ liệu
            data = yf.download(ticker, start=start_date, end=end_date, progress=False)

            if data.empty:
                print(f"[{ticker}] Không có dữ liệu mới trong khoảng thời gian từ {start_date} đến {end_date}.")
                continue

            # FIX: Đảm bảo cột là chuỗi đơn giản, phòng trường hợp yfinance trả về MultiIndex
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            # Xử lý dữ liệu
            data.reset_index(inplace=True)
            data['symbol'] = ticker
            
            # Đổi tên cột để khớp với database
            data.rename(columns={
                'Date': 'date',
                'Open': 'open_price',
                'High': 'high_price',
                'Low': 'low_price',
                'Close': 'close_price',
                'Volume': 'volume'
            }, inplace=True)

            # Chọn các cột cần thiết
            data_to_load = data[['symbol', 'date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']]

            # Ghi vào database bằng phương pháp SQLAlchemy Core để tránh lỗi pandas
            rows_to_insert = len(data_to_load)
            if rows_to_insert > 0:
                with engine.connect() as connection:
                    metadata = MetaData()
                    raw_yfinance_table = Table('raw_yfinance', metadata, autoload_with=engine)
                    
                    # Chuyển đổi DataFrame thành list of dicts
                    data_dict = data_to_load.to_dict(orient='records')
                    
                    # Bắt đầu một transaction
                    transaction = connection.begin()
                    try:
                        # Thực hiện insert
                        connection.execute(raw_yfinance_table.insert(), data_dict)
                        
                        # === BƯỚC KIỂM TRA ===
                        query = text("SELECT COUNT(*) FROM raw_yfinance WHERE symbol = :symbol AND date >= :start_date AND date <= :end_date")
                        result = connection.execute(query, {"symbol": ticker, "start_date": start_date, "end_date": end_date}).scalar()
                        
                        print(f"[{ticker}] Đã chuẩn bị {rows_to_insert} bản ghi. KIỂM TRA DB: tìm thấy {result} bản ghi mới. Commit transaction.")
                        transaction.commit()
                    except Exception as e:
                        print(f"[{ticker}] Lỗi trong transaction, rollback. Chi tiết: {e}")
                        transaction.rollback()
                        raise # Ném lại lỗi để khối except bên ngoài bắt được
            else:
                print(f"[{ticker}] Không có bản ghi mới để tải lên.")

        except Exception as e:
            print(f"[{ticker}] Lỗi trong quá trình fetch hoặc lưu dữ liệu: {e}")

    print("Hoàn tất quá trình fetch dữ liệu.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Fetch data from Yahoo Finance.")
    parser.add_argument(
        '--mode', 
        type=str, 
        default='daily', 
        choices=['daily', 'historical'],
        help="Chế độ fetch: 'daily' để cập nhật hàng ngày, 'historical' để tải lịch sử."
    )
    args = parser.parse_args()

    fetch_yfinance_data(mode=args.mode)
