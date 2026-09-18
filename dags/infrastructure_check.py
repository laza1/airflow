from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook


def check_postgres():
    hook = PostgresHook(postgres_conn_id="olist_postgres")

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()

            if result != (1,):
                raise RuntimeError("PostgreSQL check failed")

    print("PostgreSQL connection OK")


with DAG(
    dag_id="infrastructure_check",
    start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo("Indian/Antananarivo")),
    schedule=None,
    catchup=False,
    tags=["infrastructure", "healthcheck"],
) as dag:

    check_postgres_task = PythonOperator(
        task_id="check_postgres",
        python_callable=check_postgres,
        execution_timeout=timedelta(minutes=2),
    )

    trigger_pipeline = TriggerDagRunOperator(
        task_id="trigger_olist_daily_pipeline",
        trigger_dag_id="olist_daily_pipeline",
    )

    check_postgres_task >> trigger_pipeline
