from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator

from airflow_dbt_python.operators.dbt import (
    DbtRunOperator,
    DbtTestOperator,
    DbtDepsOperator
)

# -----------------------------
# Config
# -----------------------------
DBT_PROJECT_DIR = "/opt/airflow/dbt/my_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="dbt_medallion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "medallion"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. Install dependencies
    dbt_deps = DbtDepsOperator(
        task_id="dbt_deps",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    )

    # 2. Run staging models (Bronze → cleaned staging)
    dbt_run_staging = DbtRunOperator(
        task_id="dbt_run_staging",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        select=["tag:staging"],
    )

    dbt_test_staging = DbtTestOperator(
        task_id="dbt_test_staging",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        select=["tag:staging"],
    )

    # 3. Run silver models
    dbt_run_silver = DbtRunOperator(
        task_id="dbt_run_silver",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        select=["tag:silver"],
    )

    dbt_test_silver = DbtTestOperator(
        task_id="dbt_test_silver",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        select=["tag:silver"],
    )

    # 4. Run gold models
    dbt_run_gold = DbtRunOperator(
        task_id="dbt_run_gold",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        select=["tag:gold"],
    )

    dbt_test_gold = DbtTestOperator(
        task_id="dbt_test_gold",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        select=["tag:gold"],
    )

    end = EmptyOperator(task_id="end")

    # -----------------------------
    # Dependencies
    # -----------------------------
    start >> dbt_deps >> dbt_run_staging >> dbt_test_staging \
          >> dbt_run_silver >> dbt_test_silver \
          >> dbt_run_gold >> dbt_test_gold >> end
