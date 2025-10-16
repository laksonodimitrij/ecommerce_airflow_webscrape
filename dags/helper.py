from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
import time
import datetime as dt
from airflow.providers.postgres.hooks.postgres import PostgresHook

class Helper():
    
    @staticmethod
    def sephora_extract_product_data(card):
        """
        Extract product data from sephora card
        """
        product_name = card.get('data-product-name')
        product_brand = card.get('data-product-brand')

        # Price Extraction
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
        
        return {
        'upload_time': dt.datetime.now(),
        'brand': product_brand,
        'product_name': product_name,
        'price': sell_price,
        'reviews_count': reviews_count,
        'rating_percentage': rating_percentage
        }

    @staticmethod
    def sephora_get_data(ti, page_no, product_keyword):
        """
        Fetch skincare product data from Sephora Indonesia across multiple pages.
        Pushes result to Airflow's XCom.
        """

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                        '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

        all_product_data = []

        # determine page range
        start_page = 1
        end_page = max(page_no, 1)

        for page_num in range(start_page, end_page + 1):
            url = f'https://www.sephora.co.id/search?page={page_num}&q={product_keyword}'
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                results = soup.find_all('div', {'class': 'products-card-container'})

                for result in results:
                    cards = result.find_all('div', class_='product-card')
                    for card in cards:
                        product_data = Helper.sephora_extract_product_data(card)
                        all_product_data.append(product_data)

            except requests.RequestException as e:
                raise Exception(f"Failed to fetch page {page_num}: {e}")
            except Exception as e:
                raise Exception(f"Failed to parse page {page_num}: {e}")
            
            #Politeness Delay
            time.sleep(1)

        df = pd.DataFrame(all_product_data)

        # Explicitly convert any datetime type columns to ISO Strings
        for col in df.select_dtypes(include=['datetime64', 'datetimetz']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-&dT%H:%M:%S')
        
        # Also convert any remaining Timestamp or datetime objects in objects columns
        df = df.astype(object).where(pd.notnull(df), None)
        df = df.applymap(lambda x: x.isoformat() if isinstance(x, (pd.Timestamp, dt.datetime)) else x)

        ti.xcom_push(key='skincare_data', value=df.to_dict('records'))

    @staticmethod
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


    