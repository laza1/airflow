from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, "/opt/olist")

def init_observability():
    sql_file = (
        Path("/opt/olist")
        / "sql"
        / "create_pipeline_metrics.sql"
    )

    sql = sql_file.read_text(encoding="utf-8")

    hook = PostgresHook(postgres_conn_id="olist_postgres")

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)

        conn.commit()

    print("Observability infrastructure initialized successfully.")

with DAG(
    dag_id="infrastructure_provisioning",
    start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo("Indian/Antananarivo")),
    schedule=None,
    catchup=False,
    tags=["infrastructure", "provisioning"],
) as dag:

    init_observability_task = PythonOperator(
        task_id="init_observability",
        python_callable=init_observability,
        execution_timeout=timedelta(minutes=2),
    )
