select 'invalid_source' as violation
where not exists (select 1 from {{ ref('published_silver') }})
   or exists (
       select 1 from {{ ref('published_silver') }}
       where source_month is distinct from '2025-01'
          or source_file is distinct from 'yellow_tripdata_2025-01.parquet'
          or pickup_date is null
          or tpep_pickup_datetime is null
          or tpep_dropoff_datetime is null
          or trip_duration_minutes < 0
   )
union all
select 'column_count'
where (select count(*) from information_schema.columns
       where table_catalog = '{{ ref("published_silver").database }}'
         and table_schema = '{{ ref("published_silver").schema }}'
         and table_name = 'published_silver') <> 25
