'''
This is DAG to automate the information from sephora page

jobs        : fetch ecommerce products, clean data (transform), create and store data in table on postgres
operators   : Python operator and postgresoperator
hooks       : allows connections to postgres
dependencies: sequence of job steps
'''

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import timedelta
from helper import Helper
import datetime as dt


######## AIRFLOW JOBS ###########

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2024, 6, 20),  # Changed to a past date
    'retries': 2,
    'retry_delay': timedelta(minutes=4),
}

dag = DAG(
    'fetch_and_store_sephora_skincare',
    default_args=default_args,
    description='Fetching the skincare data from sephora and storing it in Postgres',
    schedule='0 7 * * *',
    catchup=False
)

start_task = EmptyOperator(task_id='start_task')

fetch_skincare_data_task = PythonOperator(
    task_id='fetch_skincare_data',
    python_callable=Helper.sephora_get_data,
    op_kwargs={'page_no':5, 'product_keyword':'masker'},
    dag=dag
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='skincare_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS skincare (
        upload_time TIMESTAMP,
        id SERIAL PRIMARY KEY,
        brand TEXT,
        product_name TEXT,
        price INTEGER,
        reviews_count INTEGER,
        rating_percentage INTEGER
    )
    """
)

insert_skincare_data_task = PythonOperator(
    task_id='insert_skincare_data',
    python_callable=Helper.insert_skincare_data_to_postgres,
    dag=dag,
)

end_task = EmptyOperator(task_id='end_task')

## dependencies
start_task >> fetch_skincare_data_task >> create_table_task >> insert_skincare_data_task >> end_task