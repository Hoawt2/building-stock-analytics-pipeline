import logging
from contextlib import closing
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_batch
import pandas as pd
from sqlalchemy import create_engine

class PostgresOperators:
    """
    Wrapper class cho PostgresHook.
    Hỗ trợ query, insert batch, upsert, load pandas DataFrame, ...
    """

    def __init__(self, conn_id="postgres_default"):
        try:
            self.conn_id = conn_id
            self.hook = PostgresHook(postgres_conn_id=self.conn_id)
        except Exception as e:
            logging.error(f"Can't connect to PostgreSQL with conn_id={conn_id}: {e}")
            raise

    # ------------------------------------------------------------
    # Basic methods
    # ------------------------------------------------------------
    def get_connection(self):
        """Lấy connection psycopg2"""
        return self.hook.get_conn()

    def get_data_to_pd(self, sql):
        """Trả kết quả query về DataFrame"""
        try:
            return self.hook.get_pandas_df(sql)
        except Exception as e:
            logging.error(f"Failed to run query: {sql} — Error: {e}")
            return pd.DataFrame()

    def execute_query(self, sql):
        """Chạy 1 câu SQL statement"""
        try:
            self.hook.run(sql)
            logging.info(f"Executed query successfully: {sql}")
        except Exception as e:
            logging.error(f"Failed to execute query: {sql} — Error: {e}")

    # ------------------------------------------------------------
    # Insert batch (tuần tự hoặc chia chunk)
    # ------------------------------------------------------------
    def insert_rows(self, table_name, rows, columns=None, page_size=10000):
        """
        Insert nhiều bản ghi theo batch.
        rows: list[tuple] hoặc list[list]
        columns: list[str]
        """
        if not rows:
            logging.warning("No data to insert.")
            return

        try:
            with closing(self.get_connection()) as conn, conn.cursor() as cur:
                cols = f"({','.join(columns)})" if columns else ""
                placeholders = ",".join(["%s"] * len(rows[0]))
                sql = f"INSERT INTO {table_name} {cols} VALUES ({placeholders})"
                execute_batch(cur, sql, rows, page_size=page_size)
                conn.commit()
                logging.info(f"Inserted {len(rows)} records into {table_name}.")
        except Exception as e:
            logging.exception(f"Error inserting rows into {table_name}: {e}")

    # ------------------------------------------------------------
    # Upsert (INSERT ... ON CONFLICT DO UPDATE)
    # ------------------------------------------------------------
    def upsert_rows(self, table_name, rows, columns, conflict_cols, page_size=10000):
        """
        Upsert nhiều bản ghi (insert or update nếu trùng khóa)
        """
        if not rows:
            logging.warning("No data to upsert.")
            return

        try:
            with closing(self.get_connection()) as conn, conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(columns))
                cols_str = ",".join(columns)
                update_stmt = ", ".join([f"{col}=EXCLUDED.{col}" for col in columns if col not in conflict_cols])
                conflict_str = ",".join(conflict_cols)
                sql = (
                    f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}) "
                    f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_stmt}"
                )
                execute_batch(cur, sql, rows, page_size=page_size)
                conn.commit()
                logging.info(f"Upserted {len(rows)} records into {table_name}.")
        except Exception as e:
            logging.exception(f"Error upserting rows into {table_name}: {e}")

    # ------------------------------------------------------------
    # Pandas DataFrame load
    # ------------------------------------------------------------
    def save_data_to_postgres(self, df, table_name, schema='public', if_exists='append', chunksize=5000):
        """Dùng pandas.to_sql để insert nhanh DataFrame"""
        try:
            conn_uri = self.hook.get_uri()
            engine = create_engine(conn_uri)
            df.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method="multi"
            )
            logging.info(f"Loaded {len(df)} rows into {schema}.{table_name}")
        except Exception as e:
            logging.exception(f"Failed to save DataFrame to {table_name}: {e}")
