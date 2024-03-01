{{
    config(
        materialized='view'
    )
}}

select 
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("integer")) }} as dispatching_base_num,
	cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }} as sr_flag,
    {{ dbt.safe_cast("Affiliated_base_number", api.Column.translate_type("integer")) }} as affiliated_base_number
from
	{{ source('staging', 'fhv_tripdata') }} 
where	
	EXTRACT(YEAR FROM pickup_datetime) = 2019



-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}