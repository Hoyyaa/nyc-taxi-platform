
{{ config(
    materialized='view',
    pre_hook="CREATE OR REPLACE TEMP SECRET nyc_s3 (
        TYPE s3,
        PROVIDER credential_chain,
        CHAIN 'config'
    )"
) }}

WITH bronze AS (
    SELECT *
    FROM read_parquet(
        's3://nyc-taxi-platform-bronze/yellow/year=2025/month=01/yellow_tripdata_2025-01.parquet'
    )
),

silver AS (
    SELECT
        -- Original business columns (20)
        VendorID,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        RatecodeID,
        store_and_fwd_flag,
        PULocationID,
        DOLocationID,
        payment_type,

        CAST(fare_amount AS DECIMAL(18,2)) AS fare_amount,
        CAST(extra AS DECIMAL(18,2)) AS extra,
        CAST(mta_tax AS DECIMAL(18,2)) AS mta_tax,
        CAST(tip_amount AS DECIMAL(18,2)) AS tip_amount,
        CAST(tolls_amount AS DECIMAL(18,2)) AS tolls_amount,
        CAST(improvement_surcharge AS DECIMAL(18,2)) AS improvement_surcharge,
        CAST(total_amount AS DECIMAL(18,2)) AS total_amount,
        CAST(congestion_surcharge AS DECIMAL(18,2)) AS congestion_surcharge,
        CAST(Airport_fee AS DECIMAL(18,2)) AS Airport_fee,
        CAST(cbd_congestion_fee AS DECIMAL(18,2)) AS cbd_congestion_fee,

        -- Lineage (2)
        '2025-01' AS source_month,
        'yellow_tripdata_2025-01.parquet' AS source_file,

        -- Derived columns (3)
        DATE_DIFF(
            'second',
            tpep_pickup_datetime,
            tpep_dropoff_datetime
        ) / 60.0 AS trip_duration_minutes,

        CAST(tpep_pickup_datetime AS DATE) AS pickup_date,

        STRFTIME(tpep_pickup_datetime, '%Y-%m') AS pickup_month

    FROM bronze

    WHERE
        tpep_pickup_datetime IS NOT NULL
        AND tpep_dropoff_datetime IS NOT NULL
        AND tpep_dropoff_datetime >= tpep_pickup_datetime
)

SELECT * FROM silver

