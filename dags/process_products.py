import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.bash import BashSensor
from shared.utils import normalize_csv, load_csv_to_postgres
from dwh.ods_prodcuts import transform_dim_products_sql
default_args = {"owner": "airflow"}


with DAG(
        dag_id="process_products",
        start_date=datetime.datetime(2021, 1, 1),
        schedule_interval="@once",
        default_args=default_args,
        catchup=False,
) as dag:
    check_csv_readiness = BashSensor(
        task_id="check_csv_readiness",
        bash_command="""
            ls /data/raw/products_{{ ds }}.csv
        """,
    )

    normalize_products_csv = PythonOperator(
        task_id='normalize_products_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/products_{{ ds }}.csv",
            'target': "/data/stg/products_{{ ds }}.csv"
        }
    )

    truncate_stg_products_table = PostgresOperator(
        task_id="truncate_stg_products_table",
        postgres_conn_id="dwh",
        sql="truncate stg_products;",
    )

    load_products_to_stg_products_table = PythonOperator(
        task_id='load_products_to_stg_products_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/products_{{ ds }}.csv",
            'table_name': 'stg_products',
            'connection_id': 'dwh'
        },
    )

    check_csv_readiness >> normalize_products_csv >> truncate_stg_products_table >> load_products_to_stg_products_table

    transform_dim_products_table = PostgresOperator(
        task_id="load_dim_products_table",
        postgres_conn_id='dwh',
        sql=transform_dim_products_sql,
    )

    load_products_to_stg_products_table >> transform_dim_products_table
