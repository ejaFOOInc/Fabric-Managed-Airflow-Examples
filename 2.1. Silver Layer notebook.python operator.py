from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime
import json
import pandas as pd
from io import BytesIO
import requests

# -----------------------------
# Config
# -----------------------------
API_ENDPOINT = "/v1/data"
ONELAKE_CONTAINER = "mycontainer"
BRONZE_PATH = "bronze/api_data/data.parquet"

FABRIC_WORKSPACE_ID = "<your_workspace_id>"
FABRIC_NOTEBOOK_ID = "<your_notebook_id>"
FABRIC_TOKEN = "<your_aad_token>"  # Ideally fetched dynamically

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
# Trigger Fabric Notebook
# -----------------------------
def run_fabric_notebook():
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{FABRIC_WORKSPACE_ID}/items/{FABRIC_NOTEBOOK_ID}/jobs"

    headers = {
        "Authorization": f"Bearer {FABRIC_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "executionData": {
            "parameters": {
                "input_path": f"Files/{BRONZE_PATH}",
                "output_table": "silver_api_data"
            }
        }
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code not in [200, 202]:
        raise Exception(f"Fabric notebook trigger failed: {response.text}")

    print("Notebook triggered successfully:", response.json())

# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="api_to_onelake_fabric_bronze_to_silver",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["fabric", "onelake", "medallion"],
) as dag:

    extract_api_data = SimpleHttpOperator(
        task_id="extract_api_data",
        http_conn_id="http_api_conn",
        endpoint=API_ENDPOINT,
        method="GET",
        headers={"Content-Type": "application/json"},
        log_response=True,
    )

    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=transform_and_upload,
        provide_context=True,
    )

    trigger_silver_notebook = PythonOperator(
        task_id="trigger_silver_notebook",
        python_callable=run_fabric_notebook,
    )

    extract_api_data >> load_bronze >> trigger_silver_notebook
