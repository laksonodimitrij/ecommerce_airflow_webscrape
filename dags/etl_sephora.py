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

from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re



######## FUNCTIONS FOR THE JOB (not yet to be revamped)###########

def get_skincare_data(ti):
    url = 'https://www.sephora.co.id/search?q=moisturizer'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        results = soup.find_all('div', {'class': 'products-card-container'})
    except Exception as e:
        raise Exception(f"Failed to fetch or parse page: {e}")

    product_data = []

    for container in results:
        product_cards = container.find_all('div', {'class': 'product-card'})
        for card in product_cards:
            product_name = card.get('data-product-name')
            product_brand = card.get('data-product-brand')

            # Price
            sell_price_text = card.find('span', class_='sell-price')
            if sell_price_text:
                sell_price_text = sell_price_text.get_text(strip=True)
                # Remove Rp, dots, spaces
                cleaned = re.sub(r'[^\d]', '', sell_price_text)  # Keeps only digits
                sell_price = int(cleaned) if cleaned.isdigit() else None
            else:
                sell_price = None

            # Reviews count
            reviews_elem = card.find('span', class_='reviews-count')
            reviews_text = reviews_elem.get_text(strip=True) if reviews_elem else 'N/A'
            reviews_count = int(reviews_text.replace('(', '').replace(')', '')) \
                if '(' in reviews_text else None

            # Extract rating percentage from style attribute
            stars_elem = card.find('div', class_='stars')
            rating_style = stars_elem['style'] if stars_elem else None
            rating_percentage = None  # Default to None

            if rating_style:
                # Use regex to extract the percentage number
                match = re.search(r'--highlighted-percentage:\s*([0-9.]+)%', rating_style)
                if match:
                    try:
                        # Convert to float first, then int (e.g., "88.000..." → 88)
                        rating_percentage = int(float(match.group(1)))
                    except ValueError:
                        rating_percentage = None  # In case conversion fails

            product_data.append({
                'brand': product_brand,
                'product_name': product_name,
                'price': sell_price,
                'reviews_count': reviews_count,
                'rating_percentage': rating_percentage
            })

    df = pd.DataFrame(product_data)
    ti.xcom_push(key='skincare_data', value=df.to_dict('records'))

def insert_skincare_data_to_postgres(ti):
    skincare_data = ti.xcom_pull(key="skincare_data", task_ids="fetch_skincare_data")
    
    if not skincare_data:
        raise ValueError("No skincare data found to insert")

    postgres_hook = PostgresHook(postgres_conn_id='skincare_connection')  # Make sure this matches your conn ID

    insert_query = """
        INSERT INTO skincare (brand, product_name, price, reviews_count, rating_percentage)
        VALUES (%s, %s, %s, %s, %s)
    """

    for item in skincare_data:
        # Clean each field
        brand = item['brand']
        product_name = item['product_name']

        # Price: ensure int or None
        price = item['price']
        price = int(price) if pd.notna(price) and price is not None else None

        # Reviews count: float → int or None
        reviews_count = item['reviews_count']
        reviews_count = int(reviews_count) if pd.notna(reviews_count) and reviews_count is not None else None

        # Rating percentage: handle NaN
        rating_percentage = item['rating_percentage']
        rating_percentage = int(rating_percentage) if pd.notna(rating_percentage) and rating_percentage is not None else None

        # Execute with cleaned values
        postgres_hook.run(
            insert_query,
            parameters=(brand, product_name, price, reviews_count, rating_percentage)
        )


######## AIRFLOW JOBS ###########

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),  # Changed to a past date
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
    python_callable=get_skincare_data,
    dag=dag
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='skincare_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS skincare (
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
    python_callable=insert_skincare_data_to_postgres,
    dag=dag,
)

end_task = EmptyOperator(task_id='end_task')

## dependencies
start_task >> fetch_skincare_data_task >> create_table_task >> insert_skincare_data_task >> end_task