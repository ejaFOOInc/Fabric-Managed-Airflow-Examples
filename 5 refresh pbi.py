from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import time

# -----------------------------
# Config
# -----------------------------
TENANT_ID = "<tenant_id>"
CLIENT_ID = "<client_id>"
CLIENT_SECRET = "<client_secret>"

GROUP_ID = "<workspace_id>"
DATASET_ID = "<dataset_id>"

# -----------------------------
# Auth
# -----------------------------
def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://analysis.windows.net/powerbi/api/.default"
    }

    response = requests.post(url, data=data)
    return response.json()["access_token"]

# -----------------------------
# Trigger refresh
# -----------------------------
def trigger_refresh(**context):
    token = get_access_token()

    url = f"https://api.powerbi.com/v1.0/myorg/groups/{GROUP_ID}/datasets/{DATASET_ID}/refreshes"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers)

    if response.status_code not in [200, 202]:
        raise Exception(f"Failed to trigger refresh: {response.text}")

    print("Refresh triggered")

# -----------------------------
# Poll status
# -----------------------------
def wait_for_refresh():
    token = get_access_token()

    url = f"https://api.powerbi.com/v1.0/myorg/groups/{GROUP_ID}/datasets/{DATASET_ID}/refreshes"

    headers = {"Authorization": f"Bearer {token}"}

    for _ in range(30):  # ~15 minutes
        response = requests.get(url, headers=headers)
        status = response.json()["value"][0]["status"]

        print("Current status:", status)

        if status == "Completed":
            return
        elif status == "Failed":
            raise Exception("Power BI refresh failed")

        time.sleep(30)

    raise Exception("Timeout waiting for refresh")

# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="powerbi_dataset_refresh",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["powerbi", "fabric", "bi"],
) as dag:

    trigger = PythonOperator(
        task_id="trigger_refresh",
        python_callable=trigger_refresh,
    )

    wait = PythonOperator(
        task_id="wait_for_refresh",
        python_callable=wait_for_refresh,
        retries=0
    )

    trigger >> wait
