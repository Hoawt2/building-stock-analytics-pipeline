# DB_CONFIG = {
#     "host": "localhost",
#     "port": 3306,
#     "user": "root",
#     "password": "hoang504901",
#     "database": "stock_bi_test"
# }
API_KEY_FMP = "TPTxrDMbZB96m9QtFClX8kGTsgub1WM3"
API_KEY_APV = "25L8UJNSALAN1R7G"
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "db"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "hoang504901"),
    "database": os.getenv("DB_NAME", "stock_bi")
}
