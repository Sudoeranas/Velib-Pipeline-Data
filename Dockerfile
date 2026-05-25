FROM apache/airflow:2.10.0-python3.11

RUN pip install --no-cache-dir \
    google-cloud-storage \
    "dbt-core==1.9.0" \
    "dbt-snowflake==1.9.0"
