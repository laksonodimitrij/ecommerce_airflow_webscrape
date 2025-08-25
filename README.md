# Ecommerce Airflow Webscrape

This project demonstrates an end-to-end ETL pipeline that scrapes product data from the first page of a skincare e-commerce website (Sephora), transforms it into a structured DataFrame, and loads it into a PostgreSQL database as a table

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

