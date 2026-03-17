from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator

column_checks = SQLColumnCheckOperator(
    task_id="column_checks",
    conn_id="fabric_sql",
    table="silver.orders_clean",
    column_mapping={
        "order_id": {
            "null_check": {"equal_to": 0}
        },
        "total_amount": {
            "min": {"geq_to": 0}
        }
    }
)
