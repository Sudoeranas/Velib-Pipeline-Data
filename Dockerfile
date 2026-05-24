FROM apache/airflow:2.10.0-python3.11

RUN pip install --no-cache-dir \
    apache-airflow-providers-google \
    google-cloud-storage \
    dbt-snowflake==1.9.0
