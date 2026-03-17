from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.hooks.sql import DbApiHook
from datetime import datetime

def log_to_db(status, message, **context):
    hook = DbApiHook.get_hook(conn_id="fabric_sql")

    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id

    sql = """
    INSERT INTO pipeline_logs (dag_id, task_id, status, message)
    VALUES (%s, %s, %s, %s)
    """

    hook.run(sql, parameters=(dag_id, task_id, status, message))

def process_with_logging(**context):
    try:
        log_to_db("STARTED", "Task started", **context)

        # Simulate work
        value = 100

        if value < 0:
            raise Exception("Negative value")

        log_to_db("SUCCESS", "Task completed", **context)

    except Exception as e:
        log_to_db("FAILED", str(e), **context)
        raise

with DAG(
    dag_id="logging_to_sql_database",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["logging", "sql"],
) as dag:

    task = PythonOperator(
        task_id="process_task",
        python_callable=process_with_logging,
        provide_context=True,
    )
