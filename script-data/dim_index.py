import pandas as pd
from db_connect import connect_db

def read_csv(csv_file):
    df = pd.read_csv(csv_file)
    return df.values.tolist()

def insert_index_data(csv_file):
    index_data = read_csv(csv_file)

    conn = connect_db()
    if not conn:
        return

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COALESCE(MAX(index_id), 0) FROM dim_index;")
        max_id = cursor.fetchone()[0]

        insert_query = """
        INSERT INTO dim_index (index_id, index_name, ticker)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE index_name = VALUES(index_name);
        """

        new_data = []
        for data in index_data:
            max_id += 1
            new_data.append((max_id, *data))

        cursor.executemany(insert_query, new_data)
        conn.commit()
        print("✅ Dữ liệu index đã được thêm/cập nhật thành công!")

    except Exception as err:
        print(f"❌ Lỗi MySQL: {err}")

    finally:
        cursor.close()
        conn.close()

def main():
    insert_index_data("index_list.csv")

if __name__ == "__main__":
    main()
