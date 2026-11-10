{{ config(materialized='table') }}

-- Preserve every retained Silver row, including identical business attributes.
select
    VendorID as vendor_id,
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    pickup_date,
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id,
    passenger_count,
    payment_type,
    trip_distance,
    trip_duration_minutes,
    coalesce(total_amount, 0::decimal(18,2)) as trip_amount
from {{ ref('published_silver') }}
