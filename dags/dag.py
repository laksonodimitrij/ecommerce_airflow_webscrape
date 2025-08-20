'''
This is DAG to automate the information from bukalapak page

jobs        : fetch ecommerce products, clean data (transform), create and store data in table on postgres
operators   : Python operator and postgresoperator
hooks       : allows connections to postgres
dependencies: 
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



######## FUNCTIONS FOR THE JOB ###########

def get_skincare_data(results):
    # Initialize a list to hold the product data
    product_data = []

    # Loop through each index in results
    for index in range(len(results)):
        # Get all of the info of the ecommerce
        product_cards = results[index].find_all('div', {'class': 'product-card'})

        # Extract data-product-brand and data-product-name 
        for card in product_cards:
            product_name = card.get('data-product-name')
            product_brand = card.get('data-product-brand')
            
            # Extract sell price
            sell_price_text = card.find('span', class_='sell-price').get_text(strip=True) if card.find('span', class_='sell-price') else 'N/A'
            sell_price = int(sell_price_text.replace('Rp. ', '').replace('.', '').strip()) if 'Rp.' in sell_price_text else 'N/A'
            
            # Extract reviews count
            reviews_count_text = card.find('span', class_='reviews-count').get_text(strip=True) if card.find('span', class_='reviews-count') else 'N/A'
            reviews_count = int(reviews_count_text.replace('(', '').replace(')', '').strip()) if '(' in reviews_count_text else 'N/A'
            
            # Extract rating
            rating = card.find('div', class_='stars')['style'] if card.find('div', class_='stars') else 'N/A'
            highlighted_percentage = 'N/A'
            
            if rating != 'N/A':
                # Use regex to find the highlighted percentage value
                match = re.search(r'--highlighted-percentage:(\d+)%', rating)
                if match:
                    highlighted_percentage = int(match.group(1))  # Convert to integer
            
            # Append the extracted data to the list
            product_data.append({
                'brand': product_brand,
                'product_name': product_name,
                'price': sell_price,
                'reviews_count': reviews_count,
                'rating_percentage': highlighted_percentage
            })

    # Create a DataFrame from the list of product data
    df = pd.DataFrame(product_data)
    
    return df

def insert_skincare_data_to_postgres(ti):
    skincare_data = ti.xcom_pull(key="skincare_data", task_ids="fetch_skincare_data")
    if not skincare_data:
        return ValueError("No Skincare Data Found")
    
    postgres_hook = PostgresHook(postgres_comm_id="ecommerce_connection")
    insert_query = """
    INSERT INTO skincare (brand, product_name, price, reviews_count, rating_percentage)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    for skincare in skincare_data:
        postgres_hook.run(insert_query, parameters=(skincare("seller"), skincare(("location"))))


####### VARIABELS ########
# big ecommerce
web_sephora = 'https://www.sephora.co.id/search?q=moisturizer' #req can
response = requests.get(web_sephora)
soup = BeautifulSoup(response.content, 'html.parser')
results = soup.find_all('div', {'class': 'products-card-container'})

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
    op_args=[results],
    dag=dag
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='ecommerce_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS skincare (
        id SERIAL PRIMARY KEY,
        brand TEXT,
        product_name TEXT,
        price NUMBER,
        reviews_count NUMBER,
        rating_percentage NUMBER
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