{{ config(materialized='table') }}
-- A NULL location is retained as one unknown-location group.
select
    pickup_location_id,
    count(*) as total_trips,
    sum(trip_amount) as total_revenue,
    avg(trip_amount) as average_trip_amount
from {{ ref('fct_trips') }}
group by pickup_location_id
