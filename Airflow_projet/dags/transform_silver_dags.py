from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd


BRONZE_BUCKET = "datalake-bronze"
# SILVER_BUCKET = "datalake-silver"

def upload_to_minio(local_path, s3_key):
    s3 = S3Hook(aws_conn_id="minio_s3")
    s3.load_file(local_path, BRONZE_BUCKET, s3_key, replace=True)

def build_silver():
    df_clients = pd.read_csv("/tmp/clients_latest.csv")
    df_ventes = pd.read_csv("/tmp/ventes_latest.csv")

    df = df_ventes.merge(df_clients, on="id_client")
    df.to_parquet("/tmp/silver_ventes.parquet")

    s3 = S3Hook("minio_s3")
    s3.load_file("/tmp/silver_ventes.parquet", "datalake-silver", "ventes/silver_ventes.parquet", replace=True)

with DAG(
    dag_id = "transformation_data_silver", 
    start_date=datetime(2026,1,1),
    description='transformation et Zone Silver',
    schedule_interval=None, 
    catchup=False
    ) as dag:

    PythonOperator(task_id="build_silver", python_callable=build_silver)

# def build_silver():
#     df_clients = pd.read_csv("bronze_clients.csv")
#     df_ventes = pd.read_csv("bronze_ventes.csv")

#     df = df_ventes.merge(df_clients, on="id_client")
#     df.to_parquet("/tmp/silver_ventes.parquet")

#     upload_to_minio("/tmp/silver_ventes.parquet", "silver/ventes/silver_ventes.parquet", bucket="datalake-silver")
