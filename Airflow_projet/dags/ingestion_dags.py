from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
# from airflow . providers . postgres . operators . postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
# from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import requests
import os

BRONZE_BUCKET = "datalake-bronze"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# def upload_to_minio(local_path, s3_key):
#     s3 = S3Hook(aws_conn_id="minio_s3")
#     s3.load_file(local_path, BRONZE_BUCKET, s3_key, replace=True)

def upload_to_minio(local_path, s3_key):
    hook = S3Hook(aws_conn_id='minio_s31')  # Connection Airflow
    # s3_key = "clients/2026/01/23/clients_20260123_113919.csv"
    hook.load_file(
        filename=local_path,
        key=s3_key,         # le chemin DANS le bucket
        bucket_name=BRONZE_BUCKET,
        replace=True
    )

# Sensors
wait_for_csv = FileSensor(
    task_id="wait_for_produits_csv",
    filepath="/opt/airflow/dags/data/produits.csv",
    poke_interval=30,
    timeout=300
)

wait_for_parquet = FileSensor(
    task_id="wait_for_stocks_parquet",
    filepath="/opt/airflow/dags/data/stocks.parquet",
    poke_interval=30,
    timeout=300
)

# Branching
def check_api_availability():
    try:
        requests.get("http://api:8000/ventes?n=1", timeout=3)
        return "extract_api"
    except:
        return "skip_api"

branch_api = BranchPythonOperator(
    task_id="check_api",
    python_callable=check_api_availability
)

skip_api = EmptyOperator(task_id="skip_api")

#  Dynamic PostgreSQL Extraction 
def extract_table(table_name, **context):
    hook = PostgresHook(postgres_conn_id="postgres_default")

    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    date_path = now.strftime("%Y/%m/%d")

    # Extraction PostgreSQL → DataFrame
    df = hook.get_pandas_df(f"SELECT * FROM {table_name}")

    # Sauvegarde locale
    local_path = f"/tmp/{table_name}_{timestamp}.csv"
    df.to_csv(local_path, index=False)

    # S3 Key = chemin dans bucket
    s3_key = f"{table_name}/{date_path}/{table_name}_{timestamp}.csv"

    # Upload MinIO
    upload_to_minio(local_path, s3_key)

    # XCom metadata
    context["ti"].xcom_push(key=f"{table_name}_bronze_path", value=s3_key)
    context["ti"].xcom_push(key=f"{table_name}_rowcount", value=len(df))


#  API Extraction 
def extract_api(**context):
    now = datetime.now()
    ts = now.strftime("%Y%m%d_%H%M%S")
    path = now.strftime("%Y/%m/%d")

    r = requests.get("http://api:8000/ventes?n=100")
    file_name = f"/tmp/api_ventes_{ts}.json"
    open(file_name, "w").write(r.text)

    s3_key = f"api_ventes/{path}/api_ventes_{ts}.json"
    upload_to_minio(file_name, s3_key)

    context["ti"].xcom_push(key="api_bronze_path", value=s3_key)

#  CSV Loading 
def load_csv(**context):
    now = datetime.now()
    ts = now.strftime("%Y%m%d_%H%M%S")
    path = now.strftime("%Y/%m/%d")

    src = "/opt/airflow/dags/data/produits.csv"
    file_name = f"/tmp/produits_{ts}.csv"
    os.system(f"cp {src} {file_name}")

    s3_key = f"produits/{path}/produits_{ts}.csv"
    print("Uploading to MinIOOOOOOOOOOOO:", file_name, "->", s3_key)
    upload_to_minio(file_name, s3_key)
    print("Upload done!!!!!!!!!!!!!!!!")

    context["ti"].xcom_push(key="csv_bronze_path", value=s3_key)

#  Parquet Loading 
def load_parquet(**context):
    now = datetime.now()
    ts = now.strftime("%Y%m%d_%H%M%S")
    path = now.strftime("%Y/%m/%d")

    src = "/opt/airflow/dags/data/stocks.parquet"
    file_name = f"/tmp/stocks_{ts}.parquet"
    os.system(f"cp {src} {file_name}")

    s3_key = f"stocks/{path}/stocks_{ts}.parquet"
    upload_to_minio(file_name, s3_key)

    context["ti"].xcom_push(key="parquet_bronze_path", value=s3_key)

#  Join task with TriggerRule
join_ingestion = EmptyOperator(
    task_id="join_ingestion",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
)

#  DAG Definition
with DAG(
    dag_id="ingestion_dag",
    default_args=default_args,
    description='Ingestion des donnees (Zone Bronze)',
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Sensors
    wait_for_csv = FileSensor(
        task_id="wait_for_produits_csv",
        filepath="/opt/airflow/dags/data/produits.csv",
        poke_interval=30,
        timeout=300
    )

    wait_for_parquet = FileSensor(
        task_id="wait_for_stocks_parquet",
        filepath="/opt/airflow/dags/data/stocks.parquet",
        poke_interval=30,
        timeout=300
    )

    # Dynamic PostgreSQL tasks
    postgres_tables = ["clients", "ventes"]
    postgres_tasks = []

    for table in postgres_tables:
        task = PythonOperator(
            task_id=f"extract_{table}",
            python_callable=extract_table,
            op_args=[table]
            # provide_context=True
        )
        postgres_tasks.append(task)

    # API branch
    branch_api >> [PythonOperator(task_id="extract_api", python_callable=extract_api), skip_api]

    # Other sources
    csv_task = PythonOperator(task_id="load_csv", python_callable=load_csv)
    parquet_task = PythonOperator(task_id="load_parquet", python_callable=load_parquet)

    # Orchestration
    wait_for_csv >> wait_for_parquet
    wait_for_parquet >> postgres_tasks
    postgres_tasks >> branch_api
    branch_api >> csv_task >> parquet_task
    [parquet_task, skip_api] >> join_ingestion
