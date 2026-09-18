from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from docker.types import Mount

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id="olist_postgres")
    return hook.get_conn()

INGESTION_TIMEOUT_MINUTES = 5
DBT_RUN_TIMEOUT_MINUTES = 10
DBT_TEST_TIMEOUT_MINUTES = 10

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="olist_daily_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    test_connection = PythonOperator(
        task_id="test_connection",
        python_callable=lambda: get_postgres_connection().close(),
    )

    # ingest_orders = BashOperator(
    #     task_id="ingest_orders",
    #     bash_command=(
    #         "cd /opt/olist && "
    #         "python scripts/ingest_raw.py "
    #         "orders olist_orders_dataset.csv"
    #     ),
    #     env={
	#     "PYTHONPATH": "/opt/olist",
    #         "POSTGRES_HOST": "host.docker.internal",
    #         "POSTGRES_PORT": "5432",
    #         "POSTGRES_DB": "olist_dw",
    #         "POSTGRES_USER": "dev",
    #         "POSTGRES_PASSWORD": "dev",
    #     },
    # )

    def run_ingest_orders():
        import sys
        sys.path.insert(0, "/opt/olist")

        from scripts.ingest_raw import ingest

        ingest(
            "orders",
            "olist_orders_dataset.csv",
            connection_factory=get_postgres_connection,
        )

    ingest_orders = PythonOperator(
        task_id="ingest_orders",
        python_callable=run_ingest_orders,
        execution_timeout=timedelta(minutes=INGESTION_TIMEOUT_MINUTES),
    )


    ###############
    def run_ingest_customers():
        import sys
        sys.path.insert(0, "/opt/olist")

        from scripts.ingest_raw import ingest

        ingest(
            "customers",
            "olist_customers_dataset.csv",
            connection_factory=get_postgres_connection,
        )

    ingest_customers = PythonOperator(
        task_id="ingest_customers",
        python_callable=run_ingest_customers,
        execution_timeout=timedelta(minutes=INGESTION_TIMEOUT_MINUTES),
    )

    ############
    def run_ingest_products():
        import sys
        sys.path.insert(0, "/opt/olist")

        from scripts.ingest_raw import ingest

        ingest(
            "products",
            "olist_products_dataset.csv",
            connection_factory=get_postgres_connection,
        )


    ingest_products = PythonOperator(
        task_id="ingest_products",
        python_callable=run_ingest_products,
        execution_timeout=timedelta(minutes=INGESTION_TIMEOUT_MINUTES),
    )

    ###########
    def run_ingest_sellers():
        import sys
        sys.path.insert(0, "/opt/olist")

        from scripts.ingest_raw import ingest

        ingest(
            "sellers",
            "olist_sellers_dataset.csv",
            connection_factory=get_postgres_connection,
        )


    ingest_sellers = PythonOperator(
        task_id="ingest_sellers",
        python_callable=run_ingest_sellers,
        execution_timeout=timedelta(minutes=INGESTION_TIMEOUT_MINUTES),
    )

    ############

    def run_ingest_order_items():
        import sys
        sys.path.insert(0, "/opt/olist")

        from scripts.ingest_raw import ingest

        ingest(
            "order_items",
            "olist_order_items_dataset.csv",
            connection_factory=get_postgres_connection,
        )


    ingest_order_items = PythonOperator(
        task_id="ingest_order_items",
        python_callable=run_ingest_order_items,
        execution_timeout=timedelta(minutes=INGESTION_TIMEOUT_MINUTES),
    )

    ##############
    def run_ingest_payments():
        import sys
        sys.path.insert(0, "/opt/olist")

        from scripts.ingest_raw import ingest

        ingest(
            "payments",
            "olist_order_payments_dataset.csv",
            connection_factory=get_postgres_connection,
        )


    ingest_payments = PythonOperator(
        task_id="ingest_payments",
        python_callable=run_ingest_payments,
        execution_timeout=timedelta(minutes=INGESTION_TIMEOUT_MINUTES),
    )

    ###########
    def run_ingest_reviews():
        import sys
        sys.path.insert(0, "/opt/olist")

        from scripts.ingest_raw import ingest

        ingest(
            "reviews",
            "olist_order_reviews_dataset.csv",
            connection_factory=get_postgres_connection,
        )


    ingest_reviews = PythonOperator(
        task_id="ingest_reviews",
        python_callable=run_ingest_reviews,
        execution_timeout=timedelta(minutes=INGESTION_TIMEOUT_MINUTES),
    )

    ###############
    def run_ingest_category_translation():
        import sys
        sys.path.insert(0, "/opt/olist")

        from scripts.ingest_raw import ingest

        ingest(
            "category_translation",
            "product_category_name_translation.csv",
            connection_factory=get_postgres_connection,
        )


    ingest_category_translation = PythonOperator(
        task_id="ingest_category_translation",
        python_callable=run_ingest_category_translation,
        execution_timeout=timedelta(minutes=INGESTION_TIMEOUT_MINUTES),
    )

    ######
    def run_ingest_geolocation():
        import sys
        sys.path.insert(0, "/opt/olist")

        from scripts.ingest_raw import ingest

        ingest(
            "geolocation",
            "olist_geolocation_dataset.csv",
            connection_factory=get_postgres_connection,
        )


    ingest_geolocation = PythonOperator(
        task_id="ingest_geolocation",
        python_callable=run_ingest_geolocation,
        execution_timeout=timedelta(minutes=INGESTION_TIMEOUT_MINUTES),
    )

    ##############

    run_dbt = DockerOperator(
        task_id="run_dbt",
        image="olist-dbt:1.0",
        command="dbt run --project-dir /opt/dbt",
        execution_timeout=timedelta(minutes=DBT_RUN_TIMEOUT_MINUTES),

        mounts=[
            Mount(
                source="/home/laza/data-learning/02-data-engineering/olist_dbt",
                target="/opt/dbt",
                type="bind",
                read_only=False,
            ),
            Mount(
                source="/home/laza/data-learning/dbt-docker/profiles",
                target="/root/.dbt",
                type="bind",
                read_only=True,
            ),
        ],

        extra_hosts={"host.docker.internal": "host-gateway"},
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image="olist-dbt:1.0",
        command="dbt test --project-dir /opt/dbt",
        execution_timeout=timedelta(minutes=DBT_TEST_TIMEOUT_MINUTES),

        mounts=[
            Mount(
                source="/home/laza/data-learning/02-data-engineering/olist_dbt",
                target="/opt/dbt",
                type="bind",
                read_only=False,
            ),
            Mount(
                source="/home/laza/data-learning/dbt-docker/profiles",
                target="/root/.dbt",
                type="bind",
                read_only=True,
            ),
        ],

        extra_hosts={"host.docker.internal": "host-gateway"},
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
    )

    #ingest_orders >> ingest_customers >> ingest_products >> ingest_sellers >> ingest_order_items >> ingest_payments >> ingest_reviews >> ingest_category_translation >> ingest_geolocation >> run_dbt >> dbt_test
    ingest_orders >> [
        ingest_customers,
        ingest_products,
        ingest_sellers,
        ingest_order_items,
        ingest_payments,
        ingest_reviews,
        ingest_category_translation,
        ingest_geolocation,
    ] >> run_dbt >> dbt_test
