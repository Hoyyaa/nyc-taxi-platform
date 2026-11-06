import duckdb

parquet_path = "data/raw/yellow_tripdata_2025-01.parquet"

con = duckdb.connect("data/warehouse/nyc_taxi.duckdb")

result = con.execute(
    "SELECT COUNT(*) FROM read_parquet(?)",
    [parquet_path],
).fetchone()

print("Total trips:", result[0])
schema = con.execute(
    "DESCRIBE SELECT * FROM read_parquet(?)",
    [parquet_path],
).fetchall()

print("\nParquet schema:")

for column in schema:
    print(column[0], "->", column[1])
con.close()