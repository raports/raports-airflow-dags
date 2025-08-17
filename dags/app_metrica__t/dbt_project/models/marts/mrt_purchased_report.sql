{{
    config(
        materialized = "distributed_incremental",
        engine="ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')",
        order_by='reporting_date, utm_source, os_name',
        partition_by='reporting_month_start_date',
        sharding_key='cityHash64(record_hash)',
        unique_key='record_hash',
        incremental_strategy='delete+insert',
    )
}}


with fct as (select * from {{ ref("fct_usage_metrics") }})

select
    MD5(
        concat(toString(reporting_date), toString(utm_source), toString(os_name))
    ) as record_hash,
    toYear(reporting_date) as reporting_year,
    toStartOfMonth(reporting_date) as reporting_month_start_date,
    reporting_date,
    utm_source,
    os_name,
    sum(purchase_sum) as purchase_sum,
    max(loaded_at) as loaded_at
from fct
group by reporting_date, utm_source, os_name
