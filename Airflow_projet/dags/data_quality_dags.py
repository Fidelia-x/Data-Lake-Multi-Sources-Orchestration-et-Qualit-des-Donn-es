# import great_expectations as ge
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

def check_ventes(**context):
    path = context['ti'].xcom_pull(
        key="ventes_bronze_path",
        task_ids="extract_postgres"
    )

    # récupération locale simplifiée
    df = pd.read_csv("/tmp/ventes_latest.csv")
    gx = ge.from_pandas(df)

    gx.expect_column_values_to_not_be_null("id_vente")
    gx.expect_column_values_to_be_between("quantite", min_value=1)
    gx.expect_column_values_to_be_between("prix", min_value=0)

    result = gx.validate()
    if not result["success"]:
        raise ValueError("Echec contrôle qualité")

with DAG(
    dag_id = "dag_quality", 
    start_date=datetime(2026,1,1),
    description='Controle de la qualite des donnees',
    schedule_interval=None, 
    catchup=False
    ) as dag:

    PythonOperator(task_id="check_ventes", python_callable=check_ventes)

# def check_ventes():
#     df = pd.read_csv("/tmp/ventes_latest.csv")
#     gx = ge.from_pandas(df)

#     gx.expect_column_values_to_not_be_null("id_vente")
#     gx.expect_column_values_to_be_between("quantite", min_value=1)
#     gx.expect_column_values_to_be_between("prix", min_value=0)

#     result = gx.validate()
#     if not result["success"]:
#         raise ValueError("Data Quality Check Failed")
