with raw as (select * from {{ source('app_metrica', 'raw_usage_metrics') }})
select
    date as reporting_date,
    event as app_event,
    purchase_sum,
    os_name,
    device_id,
    gender,
    city,
    utm_source,
    `_sling_loaded_at` as loaded_at,
    `_sling_stream_url` as source
from raw
