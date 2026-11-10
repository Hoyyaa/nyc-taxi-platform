-- GROUP BY also checks uniqueness of the nullable unknown-location group.
select pickup_location_id
from {{ ref('agg_pickup_location') }}
group by pickup_location_id
having count(*) > 1
