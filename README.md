# Skincare Product Intelligence Platform

This project demonstrates an end-to-end ETL pipeline that scrapes product data from the first page of a skincare e-commerce website (Sephora), transforms it into a structured DataFrame, and loads it into a PostgreSQL database as a table, which can be used to track the price changes for its users.

## Business Case

### Problem: 

Skincare brands and retailers must closely monitor competitor pricing, product trends, and customer sentiment to remain competitive. Manually tracking this is time-consuming and prone to errors.

### Solution: 
A scalable ETL pipeline that scrapes skincare product data from multiple e-commerce sites, enriches it with sentiment (from reviews), and enables trend analysis — all orchestrated via Airflow and stored in a structured data warehouse.

### Input:

- Product listings (name, price, brand, rating, category) from multiple e-commerce sites (e.g., Sephora, Cult Beauty, Ulta)
- Customer reviews (text + rating)

### Output:

- Clean, versioned product and review data in PostgreSQL
- Daily trend dashboards (via simple reporting views)
- Alerts for price drops or new product launches
- GitHub portfolio showing end-to-end data engineering best practices

## Requirements
- docker
- airflow v2.9.2: required as latest 3.0.0 upwards doesn't support postgres hook
- BeautifulSoup
- Requests

## Configuration
1. After running the Docker Compose containing the airflow version, do the following:
  a. Create a server and a table for the expected database in PostgreSQL, by checking the host of the table using docker inspect of the postgres inside the docker container
  b. Create a connection in Airflow by checking admin > connection and put the designated table for the data warehouse.
2. When the connection is done, the job can be run on airflow.

## Notes
v1.0:
  - This is the basic and MVP version of the airflow. Initially, I tried to use airflow 3.0.3, but modules regarding the postgreshook seem not readable on those template versions. So I stick with this version
  - Committing branches is still not as seamless or clean as I hoped to be, I still need to fix this process for better versioning/branching.
  - At the moment, the one I could gather was products from 1st page of search result from sephora.

