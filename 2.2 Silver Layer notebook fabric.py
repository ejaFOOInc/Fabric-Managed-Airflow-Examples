from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime
import json
import pandas as pd
from io import BytesIO

# 👇 Your custom Fabric operator
from airflow.providers.microsoft.fabric.operators.fabric import MSFabricRunJobOperator

# -----------------------------
# Config
# -----------------------------
API_ENDPOINT = "/v1/data"
ONELAKE_CONTAINER = "mycontainer"
BRONZE_PATH = "bronze/api_data/data.parquet"

FABRIC_WORKSPACE_ID = "<your_workspace_id>"
FABRIC_NOTEBOOK_ID = "<your_notebook_id>"

# -----------------------------
# Bronze Load
# -----------------------------
def transform_and_upload(**context):
    response = context['ti'].xcom_pull(task_ids='extract_api_data')
    data = json.loads(response)

    df = pd.json_normalize(data)

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)

    hook = WasbHook(wasb_conn_id='azure_onelake_conn')

    hook.load_bytes(
        bytes_data=buffer.getvalue(),
        container_name=ONELAKE_CONTAINER,
        blob_name=BRONZE_PATH,
        overwrite=True
    )

# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="api_to_onelake_fabric_operator",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["fabric", "onelake", "medallion"],
) as dag:

    # 1. Extract API
    extract_api_data = SimpleHttpOperator(
        task_id="extract_api_data",
        http_conn_id="http_api_conn",
        endpoint=API_ENDPOINT,
        method="GET",
        headers={"Content-Type": "application/json"},
        log_response=True,
    )

    # 2. Load Bronze
    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=transform_and_upload,
        provide_context=True,
    )

    # 3. Run Fabric Notebook → Silver
    run_silver_notebook = MSFabricRunJobOperator(
        task_id="run_silver_notebook",
        fabric_conn_id="fabric_default",  # Airflow connection
        workspace_id=FABRIC_WORKSPACE_ID,
        item_id=FABRIC_NOTEBOOK_ID,
        job_type="RunNotebook",  # or "Pipeline" if triggering a pipeline
        execution_data={
            "parameters": {
                "input_path": f"Files/{BRONZE_PATH}",
                "output_table": "silver_api_data"
            }
        },
        wait_for_termination=True,   # 👈 blocks until finished
        poll_interval=30,            # seconds
        timeout=60 * 60,             # 1 hour
    )

    extract_api_data >> load_bronze >> run_silver_notebook
