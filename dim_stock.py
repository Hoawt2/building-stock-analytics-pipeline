import pandas as pd
import yfinance as yf
from db_connect import connect_db

def get_stock_tickers(csv_file):
    df = pd.read_csv(csv_file)
    return dict(zip(df["ticker"], df["ipo_year"]))

def insert_stock_data(csv_file):
    ipo_years = get_stock_tickers(csv_file)
    tickers = list(ipo_years.keys())

    conn = connect_db()
    if not conn:
        return

    try:
        cursor = conn.cursor()

        cursor.execute("SELECT COALESCE(MAX(stock_id), 0) FROM dim_stock;")
        max_id = cursor.fetchone()[0]

        insert_query = """
        INSERT INTO dim_stock (stock_id, ticker, company_name, sector, industry, country, ipo_year)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            company_name = VALUES(company_name),
            sector = VALUES(sector),
            industry = VALUES(industry),
            country = VALUES(country),
            ipo_year = VALUES(ipo_year);
        """

        new_data = []
        for ticker in tickers:
            stock = yf.Ticker(ticker)
            info = stock.info

            company_name = info.get('longName', 'Unknown')
            sector = info.get('sector', 'Unknown')
            industry = info.get('industry', 'Unknown')
            country = info.get('country', 'Unknown')
            ipo_year = ipo_years.get(ticker, None)

            max_id += 1
            new_data.append((max_id, ticker, company_name, sector, industry, country, ipo_year))
            print(f"✅ Đã thêm/cập nhật: {ticker} - {company_name}")

        cursor.executemany(insert_query, new_data)
        conn.commit()

    except Exception as e:
        print(f"❌ Lỗi MySQL: {e}")

    finally:
        cursor.close()
        conn.close()

insert_stock_data("stock_list.csv")
