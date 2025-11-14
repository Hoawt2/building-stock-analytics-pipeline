
import os
import re
import pandas as pd
import argparse
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

# --- CONFIGURATION ---

# File .sql chứa định nghĩa các bảng của MySQL
SQL_DEFINITIONS_FILE = 'sql/extract_db.sql'

# Tên cột timestamp trong bảng MySQL để theo dõi các thay đổi.
# Dựa trên file extract_db.sql, tất cả các bảng đều có cột 'load_timestamp'.
# LƯU Ý: Để chế độ incremental hoạt động đúng với các bản ghi CẬP NHẬT,
# cột này lý tưởng nên là cột có `ON UPDATE CURRENT_TIMESTAMP`.
# Nếu nó chỉ có `DEFAULT CURRENT_TIMESTAMP`, nó sẽ chỉ hoạt động cho các bản ghi MỚI.
INCREMENTAL_TIMESTAMP_COLUMN = 'load_timestamp'

# File để lưu trữ timestamp của lần chạy cuối cùng
LAST_RUN_STATE_FILE = 'last_run_timestamp.txt'

# --- END OF CONFIGURATION ---


def get_tables_from_sql_file(filepath):
    """Đọc file .sql và trích xuất tất cả tên bảng từ các lệnh CREATE TABLE."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        # Regex để tìm tên bảng trong `CREATE TABLE IF NOT EXISTS table_name` (có hoặc không có dấu `)
        table_names = re.findall(r"CREATE TABLE IF NOT EXISTS \`?([a-zA-Z0-9_]+)\`?", content)
        if not table_names:
            print(f"Cảnh báo: Không tìm thấy lệnh 'CREATE TABLE' nào trong file {filepath}")
        return table_names
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file định nghĩa SQL tại: {filepath}")
        return []

def get_last_run_timestamp():
    """Đọc timestamp của lần chạy thành công cuối cùng từ file."""
    if not os.path.exists(LAST_RUN_STATE_FILE):
        return '1970-01-01 00:00:00'
    with open(LAST_RUN_STATE_FILE, 'r') as f:
        return f.read().strip()

def set_last_run_timestamp(timestamp):
    """Lưu timestamp của lần chạy hiện tại vào file."""
    with open(LAST_RUN_STATE_FILE, 'w') as f:
        f.write(timestamp.strftime('%Y-%m-%d %H:%M:%S'))

def main():
    """Hàm chính để thực hiện việc chuyển dữ liệu cho tất cả các bảng."""
    load_dotenv()

    # --- Thiết lập kết nối Database ---
    try:
        mysql_user = os.getenv("MYSQL_USER")
        mysql_password = os.getenv("MYSQL_PASSWORD")
        mysql_db = os.getenv("MYSQL_DATABASE")
        mysql_host = "localhost"
        mysql_port = "3307"
        mysql_engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

        postgres_user = os.getenv('POSTGRES_USER')
        postgres_password = os.getenv('POSTGRES_PASSWORD')
        postgres_host = "localhost"
        postgres_db = os.getenv('POSTGRES_DB')
        postgres_schema = 'staging'
        postgres_port = "5433"
        postgres_engine = create_engine(f'postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}'
                                        f"?options=-csearch_path%3D{postgres_schema}")
    except Exception as e:
        print(f"Lỗi kết nối database. Vui lòng kiểm tra file .env. Lỗi: {e}")
        return

    # --- Xử lý tham số và lấy danh sách bảng ---
    parser = argparse.ArgumentParser(description='Chuyển dữ liệu từ MySQL sang PostgreSQL Staging cho tất cả các bảng.')
    parser.add_argument('--mode', type=str, required=True, choices=['historical', 'incremental'],
                        help='Chế độ chạy: "historical" để tải toàn bộ, "incremental" để tải phần thay đổi.')
    args = parser.parse_args()

    mysql_tables = get_tables_from_sql_file(SQL_DEFINITIONS_FILE)
    if not mysql_tables:
        print("Không có bảng nào để xử lý. Kết thúc.")
        return

    print(f"Bắt đầu quá trình chuyển dữ liệu ở chế độ: {args.mode}")
    print(f"Các bảng sẽ được xử lý: {', '.join(mysql_tables)}")
    
    overall_success = True
    start_time = datetime.now()

    try:
        last_run_ts = get_last_run_timestamp()

        for mysql_table in mysql_tables:
            postgres_table = f"stg_{mysql_table}"
            print(f"\n--- Đang xử lý bảng: {mysql_table} -> {postgres_table} ---")

            try:
                if args.mode == 'historical':
                    print(f"Đang đọc toàn bộ dữ liệu từ {mysql_table}...")
                    query = f"SELECT * FROM {mysql_table}"
                    df = pd.read_sql(query, mysql_engine)
                    
                    print(f"Đã đọc {len(df)} dòng. Đang ghi vào {postgres_table} (chế độ replace)...")
                    df.to_sql(postgres_table, postgres_engine, schema=postgres_schema, if_exists='replace', index=False)
                    print(f"Hoàn thành historical load cho bảng {mysql_table}.")

                elif args.mode == 'incremental':
                    if last_run_ts == '1970-01-01 00:00:00':
                        print("Chạy incremental lần đầu, sẽ tải toàn bộ dữ liệu (tương tự historical).")
                        query = f"SELECT * FROM {mysql_table}"
                    else:
                        print(f"Đang tìm các bản ghi mới hơn {last_run_ts} trong bảng {mysql_table}...")
                        query = f"SELECT * FROM {mysql_table} WHERE {INCREMENTAL_TIMESTAMP_COLUMN} > '{last_run_ts}'"
                    
                    df = pd.read_sql(query, mysql_engine)

                    if df.empty:
                        print("Không có dữ liệu mới.")
                    else:
                        print(f"Tìm thấy {len(df)} dòng mới/cập nhật. Đang ghi vào {postgres_table} (chế độ append)...")
                        df.to_sql(postgres_table, postgres_engine, schema=postgres_schema, if_exists='append', index=False)
                        print(f"Đã ghi thành công {len(df)} dòng.")
            
            except Exception as e:
                print(f"!!!!!! Lỗi khi xử lý bảng {mysql_table}: {e} !!!!!!")
                overall_success = False
                continue # Chuyển sang bảng tiếp theo

        if args.mode == 'incremental' and overall_success:
            # Chỉ cập nhật timestamp nếu tất cả các bảng đều thành công
            set_last_run_timestamp(start_time)
            print(f"\nĐã cập nhật timestamp lần chạy cuối thành công: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        elif not overall_success:
            print("\nCảnh báo: Đã xảy ra lỗi trong quá trình xử lý. Timestamp lần chạy cuối chưa được cập nhật.")

    except Exception as e:
        print(f"Đã xảy ra lỗi nghiêm trọng: {e}")
    finally:
        mysql_engine.dispose()
        postgres_engine.dispose()
        print("\nĐã đóng kết nối database.")

if __name__ == '__main__':
    main()
