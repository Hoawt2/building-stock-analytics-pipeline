from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime


SCRIPT_PATH = "$AIRFLOW_HOME/script"

LIMITED_FETCH_TASKS = [
    "fetch_earnings",
    "fetch_income_statement",
    "fetch_balance_sheet",
    "fetch_cashflow",
]

@dag(
    dag_id="finance_etl_master_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["finance", "etl", "production"],
    doc_md="""
    # Master Finance ETL Pipeline
    1. **Daily Fetch**: Giá cổ phiếu, Market Cap, thông tin công ty.
    2. **Rotated Fetch**: Earnings, Cashflow, Balance Sheet, Income (luân phiên 1 cái/ngày).
    3. **Staging Load**: Đẩy dữ liệu từ Raw (MySQL) sang Staging (Postgres) incremental.
    4. **Transform**: transform sang DWH (SCD/Snapshot).
    """
)
def finance_etl_pipeline():

    def determine_fetch_task(**kwargs):
        """Chọn 1 trong 4 báo cáo tài chính dựa trên ngày trong năm."""
        execution_date = kwargs["ds_nodash"]
        day_of_year = datetime.strptime(execution_date, "%Y%m%d").timetuple().tm_yday
        
        rotation_index = day_of_year % len(LIMITED_FETCH_TASKS)
        
        selected_task = f"group_fetch_limited.{LIMITED_FETCH_TASKS[rotation_index]}"
        print(f"Hôm nay chạy: {selected_task}")
        return selected_task


    with TaskGroup("group_fetch_daily", tooltip="Fetch dữ liệu không giới hạn") as group_fetch_daily:
        
        fetch_yfinance = BashOperator(
            task_id="fetch_yfinance",
            bash_command=f"python {SCRIPT_PATH}/extract/fetch_yfinance.py --mode daily"
        )

        fetch_marketcap = BashOperator(
            task_id="fetch_marketcap",
            bash_command=f"python {SCRIPT_PATH}/extract/fetch_marketcap.py --mode daily"
        )

        fetch_company_info = BashOperator(
            task_id="fetch_company_information",
            bash_command=f"python {SCRIPT_PATH}/extract/fetch_company_information.py --mode daily"
        )

    with TaskGroup("group_fetch_limited", tooltip="Fetch luân phiên 4 báo cáo") as group_fetch_limited:
        
        # Router quyết định nhánh
        router = BranchPythonOperator(
            task_id="fetch_router",
            python_callable=determine_fetch_task,
        )

        fetch_earnings = BashOperator(
            task_id="fetch_earnings",
            bash_command=f"python {SCRIPT_PATH}/extract/fetch_earnings.py --mode daily"
        )

        fetch_income = BashOperator(
            task_id="fetch_income_statement",
            bash_command=f"python {SCRIPT_PATH}/extract/fetch_income_statement.py --mode daily"
        )

        fetch_balance = BashOperator(
            task_id="fetch_balance_sheet",
            bash_command=f"python {SCRIPT_PATH}/extract/fetch_balance_sheet.py --mode daily"
        )

        fetch_cashflow = BashOperator(
            task_id="fetch_cashflow",
            bash_command=f"python {SCRIPT_PATH}/extract/fetch_cashflow.py --mode daily"
        )

 
        router >> [fetch_earnings, fetch_income, fetch_balance, fetch_cashflow]



    with TaskGroup("group_staging", tooltip="Đẩy dữ liệu vào Staging") as group_staging:
        load_to_staging = BashOperator(
            task_id="mysql_to_postgres_staging",
            bash_command=f"python {SCRIPT_PATH}/staging/mysql_to_postgres_staging.py --mode incremental",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )


    with TaskGroup("group_transform", tooltip="Transform sang DWH") as group_transform:
        
        # Transform Dimensions
        tf_dim_company = BashOperator(
            task_id="transform_dim_company",
            bash_command=f"python {SCRIPT_PATH}/transform/transform_dim_company_information.py"
        )

        tf_fact_stock = BashOperator(
            task_id="transform_fact_stock",
            bash_command=f"python {SCRIPT_PATH}/transform/transform_fact_history_stock.py"
        )

        tf_fact_marketcap = BashOperator(
            task_id="transform_fact_market_cap",
            bash_command=f"python {SCRIPT_PATH}/transform/transform_fact_market_cap.py"
        )

        tf_fact_earnings = BashOperator(
            task_id="transform_fact_earnings",
            bash_command=f"python {SCRIPT_PATH}/transform/transform_fact_earnings.py"
        )
        
        tf_fact_cashflow = BashOperator(
            task_id="transform_fact_cash_flow",
            bash_command=f"python {SCRIPT_PATH}/transform/transform_fact_cash_flow.py"
        )
        
        tf_fact_balance = BashOperator(
            task_id="transform_fact_balance_sheet",
            bash_command=f"python {SCRIPT_PATH}/transform/transform_fact_balance_sheet.py"
        )
        
        tf_fact_income = BashOperator(
            task_id="transform_fact_income_statement",
            bash_command=f"python {SCRIPT_PATH}/transform/transform_fact_income_statement.py"
        )

        # Định nghĩa thứ tự trong Group Transform (Dim trước Fact để đảm bảo Foreign Key)
        tf_dim_company >> [
            tf_fact_stock, 
            tf_fact_marketcap, 
            tf_fact_earnings, 
            tf_fact_cashflow, 
            tf_fact_balance, 
            tf_fact_income
        ]


    [group_fetch_daily, group_fetch_limited] >> group_staging >> group_transform

finance_etl_pipeline()