FROM apache/airflow:3.0.3

USER root
RUN pip3 install apache-airflow-providers-postgres
USER airflow