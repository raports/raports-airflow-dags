# DAG: jaffle_shop__t

This DAG is designed for orchestrating Airbyte synchronization tasks for the `jaffle_shop` project. It is configured to run manually (unscheduled) and extracts data to PostgreSQL datasets used for downstream projects. 

## DAG Details
- **DAG ID:** jaffle_shop__t
- **Start Date:** 2024-10-12
- **Schedule:** None (manual trigger)
- **Owner:** ramis.khasianov
- **Email on Failure:** Enabled
- **Tags:** `airbyte`, `minio`, `postgresql`

## Tasks
- Trigger Airbyte Sync:
  - Syncs various datasets from Airbyte connections.

## Datasets Used
- `postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_customers`
- `postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_items`
- `postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_orders`
- `postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_products`
- `postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_stores`
- `postgres://postgresql.raports.net:5432/dwh.jaffle_shop.raw_supplies`

