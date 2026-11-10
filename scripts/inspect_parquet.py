from pathlib import Path
import pyarrow.parquet as pq

# Đường dẫn đến file Parquet
file_path = Path("data/raw/yellow_tripdata_2025-01.parquet")

# Mở file Parquet
parquet_file = pq.ParquetFile(file_path)

print("Parquet file opened successfully!")
print("Number of rows:", parquet_file.metadata.num_rows)
print("Number of columns:", len(parquet_file.schema_arrow))
print("Number of row groups:", parquet_file.metadata.num_row_groups)

print("\n=== PARQUET SCHEMA ===")
print(parquet_file.schema_arrow)