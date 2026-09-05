import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.csv_loader import load_csv_to_postgres
from etl.etl_loader import create_tables
from etl.observability import log_airflow_execution


# POSTGRES_CONN = (
#     "postgresql+psycopg2://postgres:postgres@postgres:5432/bankdb"
# )

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "on_success_callback": log_airflow_execution,
    "on_failure_callback": log_airflow_execution
}

# params = {
#     "postgres_conn": POSTGRES_CONN
# }

with DAG(
    dag_id="csv_to_postgres_pipeline",
    # params=params,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "postgres"]
) as dag:

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
        # op_kwargs={
        #     "conn_string": POSTGRES_CONN
        # }
    )

    load_customers = PythonOperator(
        task_id="load_customers",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            "csv_path": "/opt/airflow/data/customers_data.csv",
            "table_name": "customers",
            "load_type": "truncate",
            # "postgres_conn": POSTGRES_CONN
        }
    )

    load_cards = PythonOperator(
        task_id="load_cards",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            "csv_path": "/opt/airflow/data/cards_data.csv",
            "table_name": "cards",
            "load_type": "truncate",
            # "postgres_conn": POSTGRES_CONN
        }
    )

    load_merchants = PythonOperator(
        task_id="load_merchants",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            "csv_path": "/opt/airflow/data/merchants_table.csv",
            "table_name": "merchants",
            "load_type": "truncate",
            # "postgres_conn": POSTGRES_CONN
        }
    )

    load_transactions = PythonOperator(
        task_id="load_transactions",
        python_callable=load_csv_to_postgres,
        op_kwargs={
            "csv_path": "/opt/airflow/data/transactions_data.csv",
            "table_name": "transactions",
            "load_type": "truncate",
            # "postgres_conn": POSTGRES_CONN
        }
    )

    (
        create_tables_task >> [load_customers, load_cards, load_merchants, load_transactions]
    )