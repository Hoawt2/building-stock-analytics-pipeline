import os

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),  # Lấy từ biến môi trường, mặc định là localhost nếu không có
    "port": int(os.environ.get("DB_PORT", 3306)),    # Lấy từ biến môi trường, mặc định là 3306
    "user": os.environ.get("DB_USER", "root"),      # Lấy từ biến môi trường, mặc định là root
    "password": os.environ.get("DB_PASSWORD", "Hoang@112004"), # Lấy từ biến môi trường, mặc định
    "database": os.environ.get("DB_NAME", "stock_bi") # Lấy từ biến môi trường, mặc định
}
API_KEY_FMP = os.environ.get("API_KEY_FMP", "sU2mjPeCsIB230uLzT5UcR257V8ljpnr") # Lấy từ biến môi trường, mặc định
API_KEY_APV = os.environ.get("API_KEY_APV", "1OG5950ZT7NUWVOB") # Lấy từ biến môi trường, mặc định