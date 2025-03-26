import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "hoang504901",
    "database": "stock_bi"
}

def connect_db():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        print(f"❌ Lỗi kết nối MySQL: {e}")
        return None
