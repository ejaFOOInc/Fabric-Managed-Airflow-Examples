## ⚙️ 1. SQLExecuteQueryOperator → Execution
## ✅ Purpose
## Runs any SQL statement and does not enforce correctness of the result
## Typical uses
##  CREATE TABLE
##  INSERT / MERGE
##  UPDATE / DELETE
##  Running transformations
## Behavior
## Executes SQL
## Succeeds if query runs without error
## ❌ Does NOT check data quality

SQLExecuteQueryOperator(
    task_id="create_table",
    conn_id="fabric_sql",
    sql="""
    CREATE TABLE silver.orders_clean AS
    SELECT * FROM bronze.orders;
    """
)






## 🔍 2. SQLCheckOperator → Validation
## ✅ Purpose
## Runs a query that must return True / non-zero / valid condition
## Typical uses
##  Data quality checks
##  Assertions
##  Guardrails in pipelines
## Behavior
## Executes SQL
## Interprets result as boolean
## ❌ Fails DAG if condition is False

SQLCheckOperator(
    task_id="check_not_empty",
    conn_id="fabric_sql",
    sql="SELECT COUNT(*) > 0 FROM silver.orders_clean;"
)
