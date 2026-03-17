from airflow import DAG
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.operators.empty import EmptyOperator

# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="fabric_sql_validation_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["fabric", "sql", "validation"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. Create / transform table (example)
    create_silver_table = SQLExecuteQueryOperator(
        task_id="create_silver_table",
        conn_id="fabric_sql",
        sql="""
        CREATE TABLE IF NOT EXISTS silver.orders_clean AS
        SELECT
            order_id,
            customer_id,
            total_amount,
            order_date
        FROM bronze.orders
        WHERE total_amount IS NOT NULL;
        """
    )

    # 2. Data quality check: table not empty
    check_row_count = SQLCheckOperator(
        task_id="check_row_count",
        conn_id="fabric_sql",
        sql="""
        SELECT COUNT(*) > 0 FROM silver.orders_clean;
        """
    )

    # 3. Data quality check: no nulls in key column
    check_no_nulls = SQLCheckOperator(
        task_id="check_no_nulls",
        conn_id="fabric_sql",
        sql="""
        SELECT COUNT(*) = 0
        FROM silver.orders_clean
        WHERE order_id IS NULL;
        """
    )

    # 4. Business rule validation
    check_positive_amounts = SQLCheckOperator(
        task_id="check_positive_amounts",
        conn_id="fabric_sql",
        sql="""
        SELECT COUNT(*) = 0
        FROM silver.orders_clean
        WHERE total_amount < 0;
        """
    )

    # 5. Aggregate validation (optional)
    check_reasonable_volume = SQLCheckOperator(
        task_id="check_reasonable_volume",
        conn_id="fabric_sql",
        sql="""
        SELECT COUNT(*) > 100 FROM silver.orders_clean;
        """
    )

    end = EmptyOperator(task_id="end")

    # -----------------------------
    # Dependencies
    # -----------------------------
    start >> create_silver_table >> [
        check_row_count,
        check_no_nulls,
        check_positive_amounts,
        check_reasonable_volume
    ] >> end
