# Chinook ETL DAG

This Apache Airflow DAG performs an ETL pipeline that extracts Chinook sample database from Neon.tech and loads it to postgresql dwh

## DAG ID

`chinook__el`

## Description

This DAG loads the Chinook demo database from the Neon.tech PostgreSQL source into the postgresql_raports_io\_\_dwh raw chinook.\* tables using a Sling configuration file.
All streams are fully reloaded (mode: truncate) and written in snake_case.
The DAG runs a single sling task group, exposes resulting tables as Airflow Datasets, and can be manually triggered to refresh all raw Chinook data.
