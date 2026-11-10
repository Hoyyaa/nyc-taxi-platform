"""Manual January 2025 pipeline. Trigger only after AWS write approval.

The export flags acknowledge exclusive access to the destination keys. Airflow
serializes this DAG, but the operator must also exclude external writers. Inspect
partial S3 outputs before manually retrying a failed export.
"""

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator


PYTHON = "/opt/pipeline/venv/bin/python"
DBT = "/opt/pipeline/venv/bin/dbt"
PROJECT = "/opt/nyc-taxi"
DBT_ARGS = f"--project-dir {PROJECT}/dbt/nyc_taxi --profiles-dir {PROJECT}/dbt/nyc_taxi"

with DAG(
    dag_id="nyc_taxi_pipeline",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    # Preserve AWS/dbt settings while excluding Airflow's Python import paths.
    default_args={"retries": 0, "env": {"PYTHONPATH": ""}, "append_env": True},
) as dag:
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"{PYTHON} {PROJECT}/scripts/ingest.py",
    )
    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=f"{DBT} build {DBT_ARGS} --select silver_yellow_trips",
    )
    export_silver = BashOperator(
        task_id="export_silver",
        bash_command=(
            f'{PYTHON} {PROJECT}/scripts/export_silver.py '
            '--db-path "$NYC_TAXI_DB_PATH" --upload --single-writer'
        ),
        retries=0,
    )
    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=f"{DBT} build {DBT_ARGS} --select path:models/gold",
    )
    export_gold = BashOperator(
        task_id="export_gold",
        bash_command=(
            f'{PYTHON} {PROJECT}/scripts/export_gold.py '
            '--db-path "$NYC_TAXI_DB_PATH" --upload --single-writer '
            '--bucket nyc-taxi-platform-gold'
        ),
        retries=0,
    )

    ingest_bronze >> build_silver >> export_silver >> build_gold >> export_gold
