# raports-airflow-dags

Dags for Raports Airflow

### Running Astro

Star dev env with

```
astro dev start
```

This command will install all requirements and packages into Dockerfile, build it and run docker-compose with astro environment

Stop environment with

```
astro dev stop
```

### DBT

DBT projects are under `dags/dbt`

To run them `cd` into a projects and use this commands

- `dbt init` to initialize new project
- `dbt debug` to test connection
- `dbt run` to run your project, create models in database
- `dbt test` to run tests
- `dbt docs generate` to generate docs - `manifest.json` file
- `dbt docs serve` to launch webapp to browse docs locally
