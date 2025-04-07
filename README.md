# raports-airflow-dags

Dags for Raports Airflow

## Building image and running it

Build it with

```shell
docker buildx build --platform=linux/amd64 -t ramiskhasianov/airflow-raports:2.10.5 .
```

publish it with 

```shell
docker push ramiskhasianov/airflow-raports:2.10.5
```