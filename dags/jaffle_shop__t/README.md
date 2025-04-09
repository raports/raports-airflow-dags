# Airflow DAG: jaffle_shop__t

This file defines an Apache Airflow DAG named `jaffle_shop__t` to manage and orchestrate the DBT project workflow. 
Below is a brief overview of the functionality and components of this DAG:

## Purpose
This DAG automates DBT operations, including running transformations and generating DBT documentation, with well-defined task dependencies. 
It is primarily configured to work with a PostgreSQL database and integrates seamlessly with MinIO for storing DBT documentation.

## Key Components
1. **DBT Task Group (`run_dbt`)**:
   - Executes the DBT project transformations using the specified project and profile configurations,
   - Includes dependency installation capabilities.

2. **DBT Documentation Generation (`generate_dbt_docs_to_s3`)**:
   - Generates DBT docs,
   - Stores the output in an S3-compatible MinIO bucket.

3. **Configurations**:
   - **DBT Execution Path**: The DAG executes DBT commands using a specified virtual environment.
   - **Schedule and Retries**: Configured with custom retry policies, start dates, and a manual trigger schedule.

4. **Task Dependencies**:
   - The DAG workflow ensures that DBT transformations (`run_dbt`) must complete successfully before documentation (`generate_dbt_docs_to_s3`) is generated.
