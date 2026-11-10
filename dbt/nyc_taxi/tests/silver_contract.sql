-- Return a failing row for empty output, incorrect width, or invalid duration.
select 'empty' as violation
where not exists (select 1 from {{ ref('silver_yellow_trips') }})
union all
select 'column_count'
where (select count(*) from information_schema.columns
       where table_catalog = '{{ ref("silver_yellow_trips").database }}'
         and table_schema = '{{ ref("silver_yellow_trips").schema }}'
         and table_name = 'silver_yellow_trips') <> 25
union all
select 'negative_duration'
where exists (select 1 from {{ ref('silver_yellow_trips') }}
              where trip_duration_minutes < 0)
