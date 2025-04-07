FROM apache/airflow:2.10.5-python3.12

RUN pip install --no-cache-dir \
    airflow-clickhouse-plugin==1.4.0 \
    apache-airflow-providers-elasticsearch==6.0.0 \
    apache-airflow-providers-mongo==5.0.2 \
    apache-airflow-providers-smtp==1.9.0 \
    apache-airflow-providers-postgres==6.0.0 \
    apache-airflow-providers-redis==4.0.0 \
    apache-airflow-providers-airbyte==5.0.1 \
    apache-airflow-providers-apache-spark==5.0.1 \
    apache-airflow-providers-telegram==4.7.2 \
    apache-airflow-providers-amazon==9.2.0 \
    apache-airflow-providers-apache-kafka==1.7.0 \
    astronomer-cosmos==1.9.2 \
    sling==1.4.4 \
    s3fs==2025.3.0

RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres==1.9.0 dbt-clickhouse==1.8.9 && \
    deactivate