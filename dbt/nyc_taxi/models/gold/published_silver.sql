{{ config(materialized='view', pre_hook=("CREATE OR REPLACE TEMP SECRET nyc_gold_s3 (TYPE s3, PROVIDER credential_chain, CHAIN 'config')" if var('silver_parquet_path', 's3://nyc-taxi-platform-silver/trips/source_month=2025-01/yellow_trips.parquet').startswith('s3://') else [])) }}

select * from read_parquet('{{ var("silver_parquet_path", "s3://nyc-taxi-platform-silver/trips/source_month=2025-01/yellow_trips.parquet") | replace("'", "''") }}')
