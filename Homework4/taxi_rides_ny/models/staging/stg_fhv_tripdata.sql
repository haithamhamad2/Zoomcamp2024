{{
    config(
        materialized='view'
    )
}}

with tripdata as 
( 
    select
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime, 
    from {{ source('staging', 'fhv_tripdata') }} 
    where EXTRACT(YEAR FROM pickup_datetime) = 2019 )




-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}