import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.bash import BashSensor
from shared.utils import normalize_csv, load_csv_to_postgres
from dwh.ods_orders import transform_dim_orders_sql, transform_fact_orders_created_sql

default_args = {"owner": "airflow"}


with DAG(
        dag_id="process_orders",
        start_date=datetime.datetime(2021, 1, 1),
        schedule_interval="@once",
        default_args=default_args,
        catchup=False,
) as dag:
    check_stg_orders_csv_readiness = BashSensor(
        task_id="check_stg_orders_csv_readiness",
        bash_command="""
               ls /data/raw/orders_{{ ds }}.csv
           """,
    )

    normalize_orders_csv = PythonOperator(
        task_id='normalize_orders_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/orders_{{ ds }}.csv",
            'target': "/data/stg/orders_{{ ds }}.csv"
        }
    )

    truncate_stg_orders_table = PostgresOperator(
        task_id="truncate_stg_orders_table",
        postgres_conn_id="dwh",
        sql="truncate stg_orders;",
    )

    load_orders_to_stg_orders_table = PythonOperator(
        task_id='load_orders_to_stg_orders_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/orders_{{ ds }}.csv",
            'table_name': 'stg_orders',
            'connection_id': 'dwh'
        },
    )

    transform_dim_orders_table = PostgresOperator(
        task_id="transform_dim_orders_table",
        postgres_conn_id='dwh',
        sql=transform_dim_orders_sql,
    )

    transform_fact_orders_created_table = PostgresOperator(
        task_id="transform_fact_orders_created_table",
        postgres_conn_id='dwh',
        sql=transform_fact_orders_created_sql,
    )

    check_stg_orders_csv_readiness >> normalize_orders_csv >> truncate_stg_orders_table
    truncate_stg_orders_table >> load_orders_to_stg_orders_table
    load_orders_to_stg_orders_table >> [transform_dim_orders_table, transform_fact_orders_created_table]
