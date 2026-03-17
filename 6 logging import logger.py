from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

def process_data(**context):
    logger = logging.getLogger("airflow.task")

    logger.info("Starting data processing")

    try:
        # Simulate work
        result = 42

        logger.info(f"Processing result: {result}")

        if result < 0:
            raise ValueError("Invalid result")

        logger.info("Processing completed successfully")

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise

with DAG(
    dag_id="logging_with_python_logger",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["logging"],
) as dag:

    task = PythonOperator(
        task_id="process_task",
        python_callable=process_data,
        provide_context=True,
    )
