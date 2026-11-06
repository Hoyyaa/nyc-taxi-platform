import duckdb

db_path = "data/warehouse/nyc_taxi.duckdb"

con = duckdb.connect(db_path)

result = con.execute("SELECT 1 + 1").fetchone()

print("DuckDB version:", duckdb.__version__)
print("SQL result:", result[0])
print("Database:", db_path)

con.close()