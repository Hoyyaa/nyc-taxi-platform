-- Exact decimal revenue reconciliation; a failing row identifies the invariant.
select 'fact_count' as violation
where (select count(*) from {{ ref('fct_trips') }})
   <> (select count(*) from {{ ref('published_silver') }})
union all
select 'fact_revenue'
where (select coalesce(sum(trip_amount), 0) from {{ ref('fct_trips') }})
   <> (select coalesce(sum(total_amount), 0) from {{ ref('published_silver') }})
{% for model in ['agg_daily_trips', 'agg_pickup_location'] %}
union all
select '{{ model }}_count'
where (select coalesce(sum(total_trips), 0) from {{ ref(model) }})
   <> (select count(*) from {{ ref('fct_trips') }})
union all
select '{{ model }}_revenue'
where (select coalesce(sum(total_revenue), 0) from {{ ref(model) }})
   <> (select coalesce(sum(trip_amount), 0) from {{ ref('fct_trips') }})
{% endfor %}
