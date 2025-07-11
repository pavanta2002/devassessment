#secret key of Azure

# COMMAND ----------

import time
from pyspark.sql import functions as F


df = spark.read.table("poc_dev.pavanta_dec.transactions")

CHUNK_SIZE = 10000
OUTPUT_DIR = "abfss://incrementalloading2@btaze1idefcsa02.dfs.core.windows.net/chunks"
total_rows = df.count()
num_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE


df_with_idx = df.withColumn("row_id", F.monotonically_increasing_id())

for i in range(num_chunks):
    start_idx = i * CHUNK_SIZE
    end_idx = min(start_idx + CHUNK_SIZE, total_rows)
    chunk = (
        df_with_idx
        .filter((F.col("row_id") >= start_idx) & (F.col("row_id") < end_idx))
        .drop("row_id")
    )
    chunk_path = f"{OUTPUT_DIR}/chunk_{i+1}.csv"
    (chunk
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(chunk_path))
    print(f"Wrote chunk {i+1} ({start_idx}-{end_idx}) to {chunk_path}")
    time.sleep(1)

print("All chunks written.")