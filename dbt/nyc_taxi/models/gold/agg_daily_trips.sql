{{ config(materialized='table') }}
select
    pickup_date,
    count(*) as total_trips,
    sum(trip_amount) as total_revenue,
    avg(trip_amount) as average_trip_amount,
    avg(trip_distance) as average_trip_distance,
    avg(trip_duration_minutes) as average_trip_duration
from {{ ref('fct_trips') }}
group by pickup_date
