from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]

DB_PATH = ROOT / "data/warehouse/nyc_taxi.duckdb"
PARQUET_PATH = ROOT / "data/raw/yellow_tripdata_2025-01.parquet"

with duckdb.connect(str(DB_PATH)) as con:

    result = con.execute(
        """
        SELECT
            COUNT(*) AS total_rows,
            MIN(tpep_pickup_datetime) AS min_pickup,
            MAX(tpep_pickup_datetime) AS max_pickup
        FROM read_parquet(?)
        """,
        [str(PARQUET_PATH)],
    ).fetchone()

    month_result = con.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE tpep_pickup_datetime < '2025-01-01'
            ) AS before_january,

            COUNT(*) FILTER (
                WHERE tpep_pickup_datetime >= '2025-01-01'
                AND tpep_pickup_datetime < '2025-02-01'
            ) AS in_january,

            COUNT(*) FILTER (
                WHERE tpep_pickup_datetime >= '2025-02-01'
            ) AS after_january,

            COUNT(*) FILTER (
                WHERE tpep_pickup_datetime IS NULL
            ) AS null_pickup

        FROM read_parquet(?)
        """,
        [str(PARQUET_PATH)],
    ).fetchone()

    distance_quality = con.execute(
        """
        SELECT
            COUNT(*) AS total_trips,

            COUNT(*) FILTER (
                WHERE trip_distance = 0
            ) AS zero_distance,

            COUNT(*) FILTER (
                WHERE trip_distance < 0
            ) AS negative_distance,

            COUNT(*) FILTER (
                WHERE trip_distance > 100
            ) AS over_100_miles

        FROM read_parquet(?)
        """,
        [str(PARQUET_PATH)],
    ).fetchone()

    percentile_result = con.execute(
        """
        SELECT
            ROUND(AVG(trip_distance), 2) AS avg_distance,
            ROUND(QUANTILE_CONT(trip_distance, 0.50), 2) AS median_distance,
            ROUND(QUANTILE_CONT(trip_distance, 0.95), 2) AS p95_distance,
            ROUND(QUANTILE_CONT(trip_distance, 0.99), 2) AS p99_distance,
            MAX(trip_distance) AS max_distance
        FROM read_parquet(?)
        """,
        [str(PARQUET_PATH)],
    ).fetchone()

    columns_info = con.execute(
        """
        DESCRIBE SELECT *
        FROM read_parquet(?)
        """,
        [str(PARQUET_PATH)],
    ).fetchall()
    column_names = [row[0] for row in columns_info]

    # Quote identifiers so column names remain valid SQL, including special characters.
    null_expressions = [
        'COUNT(*) - COUNT("' + column.replace('"', '""') + '")'
        for column in column_names
    ]

    null_result = con.execute(
        "SELECT COUNT(*), " + ", ".join(null_expressions) + " FROM read_parquet(?)",
        [str(PARQUET_PATH)],
    ).fetchone()

    all_null_result = con.execute(
        """
        SELECT COUNT(*)
        FROM read_parquet(?)
        WHERE passenger_count IS NULL
          AND RatecodeID IS NULL
          AND store_and_fwd_flag IS NULL
          AND congestion_surcharge IS NULL
          AND Airport_fee IS NULL
        """,
        [str(PARQUET_PATH)],
    ).fetchone()

    distance_result = con.execute(
        """
        SELECT
            MIN(trip_distance) AS min_distance,
            MAX(trip_distance) AS max_distance,
            ROUND(AVG(trip_distance), 2) AS avg_distance
        FROM read_parquet(?)
        """,
        [str(PARQUET_PATH)],
    ).fetchone()

    outlier_impact = con.execute(
        """
        SELECT
            ROUND(
                AVG(trip_distance), 2
            ) AS avg_all,

            ROUND(
                AVG(trip_distance) FILTER (
                    WHERE trip_distance <= 100
                ), 2
            ) AS avg_without_over_100,

            ROUND(
                SUM(trip_distance), 2
            ) AS total_distance_all,

            ROUND(
                SUM(trip_distance) FILTER (
                    WHERE trip_distance > 100
                ), 2
            ) AS total_distance_outliers

        FROM read_parquet(?)
        """,
        [str(PARQUET_PATH)],
    ).fetchone()

    duration_quality = con.execute(
        """
        WITH trips AS (
            SELECT
                date_diff(
                    'second',
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime
                ) AS duration_seconds
            FROM read_parquet(?)
        )
        SELECT
            COUNT(*) AS total_trips,

            COUNT(*) FILTER (
                WHERE duration_seconds < 0
            ) AS negative_duration,

            COUNT(*) FILTER (
                WHERE duration_seconds = 0
            ) AS zero_duration,

            COUNT(*) FILTER (
                WHERE duration_seconds > 0
            ) AS positive_duration

        FROM trips
        """,
        [str(PARQUET_PATH)],
    ).fetchone()

    vendor_result = con.execute(
        """
        SELECT
            VendorID,
            COUNT(*) AS total_trips,

            COUNT(*) FILTER (
                WHERE passenger_count IS NULL
            ) AS null_trips,

            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE passenger_count IS NULL
                ) / COUNT(*),
                2
            ) AS null_rate_pct

        FROM read_parquet(?)
        GROUP BY VendorID
        ORDER BY VendorID
        """,
        [str(PARQUET_PATH)],
    ).fetchall()
    financial_quality = con.execute(
        """
        SELECT
            COUNT(*) AS total_trips,

            COUNT(*) FILTER (
                WHERE fare_amount < 0
            ) AS negative_fare,

            COUNT(*) FILTER (
                WHERE fare_amount = 0
            ) AS zero_fare,

            COUNT(*) FILTER (
                WHERE total_amount < 0
            ) AS negative_total,

            COUNT(*) FILTER (
                WHERE total_amount = 0
            ) AS zero_total,

            COUNT(*) FILTER (
                WHERE tip_amount < 0
            ) AS negative_tip

        FROM read_parquet(?)
        """,
        [str(PARQUET_PATH)],
    ).fetchone()
    financial_quality = con.execute(
    """
    SELECT
        COUNT(*) AS total_trips,

        COUNT(*) FILTER (
            WHERE fare_amount < 0
        ) AS negative_fare,

        COUNT(*) FILTER (
            WHERE fare_amount = 0
        ) AS zero_fare,

        COUNT(*) FILTER (
            WHERE total_amount < 0
        ) AS negative_total,

        COUNT(*) FILTER (
            WHERE total_amount = 0
        ) AS zero_total,

        COUNT(*) FILTER (
            WHERE tip_amount < 0
        ) AS negative_tip

    FROM read_parquet(?)
    """,
    [str(PARQUET_PATH)],
).fetchone()

    business_key_result = con.execute(
    """
    WITH key_groups AS (
        SELECT
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            PULocationID,
            DOLocationID,
            COUNT(*) AS row_count
        FROM read_parquet(?)
        GROUP BY
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            PULocationID,
            DOLocationID
    )
    SELECT
        COUNT(*) FILTER (
            WHERE row_count > 1
        ) AS duplicate_groups,

        COALESCE(
            SUM(row_count - 1) FILTER (
                WHERE row_count > 1
            ), 0
        ) AS duplicate_excess_rows,

        MAX(row_count) AS max_group_size
    FROM key_groups
    """,
    [str(PARQUET_PATH)],
).fetchone()

    financial_distribution = con.execute(
    """
    SELECT
        ROUND(MIN(fare_amount), 2) AS min_fare,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        ROUND(QUANTILE_CONT(fare_amount, 0.50), 2) AS median_fare,
        ROUND(QUANTILE_CONT(fare_amount, 0.99), 2) AS p99_fare,
        ROUND(MAX(fare_amount), 2) AS max_fare,

        ROUND(MIN(total_amount), 2) AS min_total,
        ROUND(AVG(total_amount), 2) AS avg_total,
        ROUND(QUANTILE_CONT(total_amount, 0.50), 2) AS median_total,
        ROUND(QUANTILE_CONT(total_amount, 0.99), 2) AS p99_total,
        ROUND(MAX(total_amount), 2) AS max_total,

        ROUND(MIN(tip_amount), 2) AS min_tip,
        ROUND(AVG(tip_amount), 2) AS avg_tip,
        ROUND(QUANTILE_CONT(tip_amount, 0.50), 2) AS median_tip,
        ROUND(QUANTILE_CONT(tip_amount, 0.99), 2) AS p99_tip,
        ROUND(MAX(tip_amount), 2) AS max_tip

    FROM read_parquet(?)
    """,
    [str(PARQUET_PATH)],
).fetchone()

    financial_impact = con.execute(
    """
    SELECT
        COUNT(*) FILTER (
            WHERE total_amount > 1000
        ) AS high_value_trips,

        ROUND(SUM(total_amount), 2) AS total_all,

        ROUND(
            SUM(total_amount) FILTER (
                WHERE total_amount > 1000
            ), 2
        ) AS total_high_value,

        ROUND(
            AVG(total_amount) FILTER (
                WHERE total_amount <= 1000
            ), 2
        ) AS avg_without_high_value

    FROM read_parquet(?)
    """,
    [str(PARQUET_PATH)],
).fetchone()
    duplicate_result = con.execute(
    """
    WITH unique_rows AS (
        SELECT DISTINCT *
        FROM read_parquet(?)
    )
    SELECT
        (SELECT COUNT(*) FROM read_parquet(?)) AS total_rows,
        (SELECT COUNT(*) FROM unique_rows) AS distinct_rows
    """,
    [str(PARQUET_PATH), str(PARQUET_PATH)],
).fetchone()

    long_duration = con.execute(
        """
        WITH trips AS (
            SELECT
                date_diff(
                    'second',
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime
                ) / 60.0 AS duration_minutes
            FROM read_parquet(?)
        )
        SELECT
            ROUND(MIN(duration_minutes), 2) AS min_minutes,

            ROUND(
                AVG(duration_minutes) FILTER (
                    WHERE duration_minutes > 0
                ), 2
            ) AS avg_positive_minutes,

            ROUND(
                QUANTILE_CONT(duration_minutes, 0.50) FILTER (
                    WHERE duration_minutes > 0
                ),
                2
            ) AS median_minutes,

            ROUND(
                QUANTILE_CONT(duration_minutes, 0.95) FILTER (
                    WHERE duration_minutes > 0
                ),
                2
            ) AS p95_minutes,

            ROUND(
                QUANTILE_CONT(duration_minutes, 0.99) FILTER (
                    WHERE duration_minutes > 0
                ),
                2
            ) AS p99_minutes,

            ROUND(MAX(duration_minutes), 2) AS max_minutes,

            COUNT(*) FILTER (
                WHERE duration_minutes > 360
            ) AS over_6_hours

        FROM trips
        """,
        [str(PARQUET_PATH)],
    ).fetchone()

print("\n--- NULL BY VENDOR ---")

for row in vendor_result:
    print(
        f"VendorID={row[0]}, "
        f"Total={row[1]}, "
        f"NULL={row[2]}, "
        f"Rate={row[3]}%"
    )

print("\n--- TRIP DISTANCE PROFILING ---")
print("Min distance:", distance_result[0])
print("Max distance:", distance_result[1])
print("Avg distance:", distance_result[2])

print("\n--- NULL PATTERN ---")
print("All 5 columns NULL:", all_null_result[0])

print("\n--- COLUMN NAMES ---")
print("Total columns:", len(column_names))

for column in column_names:
    print(column)

print("\n--- NULL PROFILING ---")
print("Total rows:", null_result[0])
for column, null_count in zip(column_names, null_result[1:]):
    print(f"NULL {column}: {null_count}")

print("\n--- PICKUP DATE PROFILING ---")
print("Before January:", month_result[0])
print("In January:", month_result[1])
print("After January:", month_result[2])
print("NULL pickup:", month_result[3])
print("Total rows:", result[0])
print("Min pickup:", result[1])
print("Max pickup:", result[2])

print("\n--- DISTANCE PERCENTILES ---")
print("AVG:", percentile_result[0])
print("Median (P50):", percentile_result[1])
print("P95:", percentile_result[2])
print("P99:", percentile_result[3])
print("MAX:", percentile_result[4])

print("\n--- DISTANCE QUALITY ---")
print("Total trips:", distance_quality[0])
print("Zero distance:", distance_quality[1])
print("Negative distance:", distance_quality[2])
print("Over 100 miles:", distance_quality[3])

print("\n--- OUTLIER IMPACT ---")
print("AVG all:", outlier_impact[0])
print("AVG without >100:", outlier_impact[1])
print("Total distance all:", outlier_impact[2])
print("Total distance >100:", outlier_impact[3])

print("\n--- DURATION QUALITY ---")
print("Total trips:", duration_quality[0])
print("Negative duration:", duration_quality[1])
print("Zero duration:", duration_quality[2])
print("Positive duration:", duration_quality[3])

print("\n--- LONG DURATION ANALYSIS ---")
print("Min duration:", long_duration[0])
print("AVG positive:", long_duration[1])
print("Median positive:", long_duration[2])
print("P95 positive:", long_duration[3])
print("P99 positive:", long_duration[4])
print("Max duration:", long_duration[5])
print("Over 6 hours:", long_duration[6])

print("\n--- FINANCIAL QUALITY ---")
print("Total trips:", financial_quality[0])
print("Negative fare:", financial_quality[1])
print("Zero fare:", financial_quality[2])
print("Negative total:", financial_quality[3])
print("Zero total:", financial_quality[4])
print("Negative tip:", financial_quality[5])

print("\n--- FINANCIAL DISTRIBUTION ---")

columns = [
    "Min fare", "AVG fare", "Median fare", "P99 fare", "Max fare",
    "Min total", "AVG total", "Median total", "P99 total", "Max total",
    "Min tip", "AVG tip", "Median tip", "P99 tip", "Max tip",
]

for name, value in zip(columns, financial_distribution):
    print(f"{name}: {value}")

print("\n--- FINANCIAL IMPACT ---")
print("Trips > $1000:", financial_impact[0])
print("Total amount all:", financial_impact[1])
print("Total amount > $1000:", financial_impact[2])
print("AVG without > $1000:", financial_impact[3])

total_rows, distinct_rows = duplicate_result
duplicate_rows = total_rows - distinct_rows
duplicate_rate = duplicate_rows / total_rows * 100

print("\n--- EXACT DUPLICATE PROFILING ---")
print("Total rows:", total_rows)
print("Distinct rows:", distinct_rows)
print("Duplicate excess rows:", duplicate_rows)
print("Duplicate rate:", round(duplicate_rate, 4), "%")

print("\n--- BUSINESS KEY PROFILING ---")
print("Duplicate groups:", business_key_result[0])
print("Duplicate excess rows:", business_key_result[1])
print("Max group size:", business_key_result[2])