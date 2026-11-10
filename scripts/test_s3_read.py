import duckdb

db_path = "data/warehouse/nyc_taxi.duckdb"

con = duckdb.connect(db_path, read_only=True)

try:
    result = con.execute("""
        SELECT
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_name = 'silver_yellow_trips'
    """).fetchall()

    print("Silver objects:", result)

finally:
    con.close()