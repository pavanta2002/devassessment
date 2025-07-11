#secret key of Azure
# COMMAND ----------

import time

CHUNK_SOURCE_PATH = "abfss://incrementalloading2@btaze1idefcsa02.dfs.core.windows.net/chunks"


processed_chunks = set()

def list_new_chunks(path, already_processed):
    try:
        chunk_dirs = dbutils.fs.ls(path)
    except Exception as e:
        print(f"Error listing files in {path}: {e}")
        return []

    # Only look for directories ending with '.csv/'
    new_chunk_dirs = [f.path for f in chunk_dirs if f.path not in already_processed and f.path.endswith('.csv/')]
    actual_csv_files = []
    for chunk_dir in new_chunk_dirs:
        try:
            files_in_chunk = dbutils.fs.ls(chunk_dir)
            # Find the actual CSV file inside 
            csv_files = [f.path for f in files_in_chunk if f.path.endswith('.csv')]
            if csv_files:
                actual_csv_files.append(csv_files[0])  
        except Exception as e:
            print(f"Error listing files in {chunk_dir}: {e}")
    return actual_csv_files, new_chunk_dirs

while True:
    csv_files, new_chunk_dirs = list_new_chunks(CHUNK_SOURCE_PATH, processed_chunks)
    if csv_files:
        print(f"Found new chunk files: {csv_files}")
        # Here you can add your data processing logic for each csv_file
    else:
        print("No new chunk files found.")
    # Mark chunk dirs as processed (so you don't re-process)
    for chunk_dir in new_chunk_dirs:
        processed_chunks.add(chunk_dir)
    time.sleep(10)

# COMMAND ----------

# Load customer importance reference data from Databricks table
importance_df = spark.table("poc_dev.pavanta_dec.customer_importance")
importance_df.show(5)  # Preview first 5 rows

# COMMAND ----------

import time

CHUNK_SOURCE_PATH = "abfss://incrementalloading2@btaze1idefcsa02.dfs.core.windows.net/chunks"

# Load the customer importance reference table once (outside the loop for efficiency)
importance_df = spark.table("poc_dev.pavanta_dec.customer_importance")

# List all chunk directories (each ends with '.csv/')
chunk_dirs = [f.path for f in dbutils.fs.ls(CHUNK_SOURCE_PATH) if f.path.endswith('.csv/')]

if not chunk_dirs:
    print("No chunk directories found in the directory!")
else:
    for chunk_dir in chunk_dirs:
        print(f"Processing chunk directory: {chunk_dir}")

        # List files inside the chunk directory and find the actual CSV file
        files_in_chunk = dbutils.fs.ls(chunk_dir)
        csv_files = [f.path for f in files_in_chunk if f.path.endswith('.csv')]
        
        if not csv_files:
            print(f"No CSV file found in {chunk_dir}")
            continue

        actual_csv_path = csv_files[0]  # If there are multiple, take the first one

        print(f"Reading CSV file: {actual_csv_path}")

        # Read the CSV file as a Spark DataFrame
        chunk_df = spark.read.option("header", True).csv(actual_csv_path)

        print(f"Preview of chunk data from {actual_csv_path}:")
        chunk_df.show(3)

        # Join chunk with importance table on customer, merchant, and category
        joined_df = chunk_df.join(
            importance_df,
            (chunk_df.customer == importance_df.Source) &
            (chunk_df.merchant == importance_df.Target) &
            (chunk_df.category == importance_df.typeTrans),
            how="left"
        )

        print(f"Preview of joined data for {actual_csv_path}:")
        joined_df.show(3)



# COMMAND ----------


CHUNK_SOURCE_PATH = "abfss://incrementalloading2@btaze1idefcsa02.dfs.core.windows.net/chunks"


chunk_dirs = [f.path for f in dbutils.fs.ls(CHUNK_SOURCE_PATH) if f.path.endswith('.csv/')]

if not chunk_dirs:
    print("No chunk directories found in the directory!")
else:
    # Pick the first chunk directory for demonstration
    first_chunk_dir = chunk_dirs[0]
    print(f"Reading chunk from directory: {first_chunk_dir}")

    # List files inside the chunk directory and find the actual CSV file
    files_in_chunk = dbutils.fs.ls(first_chunk_dir)
    csv_files = [f.path for f in files_in_chunk if f.path.endswith('.csv')]

    if not csv_files:
        print(f"No CSV file found in {first_chunk_dir}")
    else:
        actual_csv_path = csv_files[0]
        print(f"Reading CSV file: {actual_csv_path}")
        
        # Read chunk file
        chunk_df = spark.read.option("header", True).csv(actual_csv_path)
        
        # Load reference table
        importance_df = spark.table("poc_dev.pavanta_dec.customer_importance")
        
        # Join chunk and importance table
        joined_df = chunk_df.join(
            importance_df,
            (chunk_df.customer == importance_df.Source) &
            (chunk_df.merchant == importance_df.Target) &
            (chunk_df.category == importance_df.typeTrans),
            how="left"
        )
        joined_df.show(3)

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd
import uuid
from datetime import datetime
import pytz


OUTPUT_BASE_PATH = "abfss://upgrade@btaze1idefcsa02.dfs.core.windows.net/chunks" 
PATTERN_ID = "PatId1"
ACTION_TYPE = "UPGRADE"

def get_current_ist_time():
    utc_now = datetime.utcnow()
    ist = pytz.timezone('Asia/Kolkata')
    return utc_now.replace(tzinfo=pytz.utc).astimezone(ist).strftime("%Y-%m-%d %H:%M:%S")

# --- MAIN LOGIC ---
YStartTime = get_current_ist_time()

merchant_stats = joined_df.groupBy("merchant").agg(F.count("*").alias("merchant_txn_count"))
merchants_over_50k = merchant_stats.filter(F.col("merchant_txn_count") > 50000).select("merchant")

upgrade_detections = []

for merchant_row in merchants_over_50k.collect():
    merchant_id = merchant_row["merchant"]
    merchant_customers = joined_df.filter(F.col("merchant") == merchant_id) \
        .groupBy("customer") \
        .agg(
            F.count("*").alias("customer_txn_count"),
            F.avg("Weight").alias("customer_avg_weight"),
        )
    total_customers = merchant_customers.count()
    top_n = max(1, int(total_customers * 0.1))
    bottom_n = max(1, int(total_customers * 0.1))
    top_10p = merchant_customers.orderBy(F.desc("customer_txn_count")).limit(top_n)
    bottom_10p = merchant_customers.orderBy(F.asc("customer_avg_weight")).limit(bottom_n)
    top_customers = set([r["customer"] for r in top_10p.collect()])
    bottom_customers = set([r["customer"] for r in bottom_10p.collect()])
    upgrade_customers = top_customers & bottom_customers

    for cust in upgrade_customers:
        detectionTime = get_current_ist_time()
        upgrade_detections.append({
            "YStartTime": YStartTime,
            "detectionTime": detectionTime,
            "patternId": PATTERN_ID,
            "ActionType": ACTION_TYPE,
            "customerName": cust if cust is not None else "",
            "MerchantId": merchant_id if merchant_id is not None else ""
        })

print(f"Upgrade detections found: {len(upgrade_detections)}")
for det in upgrade_detections[:5]:
    print(det)


if upgrade_detections:
    upgrade_pd = pd.DataFrame(upgrade_detections)
    for i in range(0, len(upgrade_pd), 50):
        batch = upgrade_pd.iloc[i:i+50]
        batch_id = uuid.uuid4().hex
        batch_path = f"{OUTPUT_BASE_PATH}upgrade_detections_{batch_id}.csv"
        batch_spark = spark.createDataFrame(batch)
        batch_spark.coalesce(1).write.mode("overwrite").option("header", True).csv(batch_path)
        print(f"Wrote {len(batch)} upgrade detections to {batch_path}")
else:
    print("No upgrade detections found.")

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd
import uuid
from datetime import datetime
import pytz


OUTPUT_BASE_PATH = "abfss://child@btaze1idefcsa02.dfs.core.windows.net/chunks"  
PATTERN_ID = "PatId2"
ACTION_TYPE = "CHILD"

def get_current_ist_time():
    utc_now = datetime.utcnow()
    ist = pytz.timezone('Asia/Kolkata')
    return utc_now.replace(tzinfo=pytz.utc).astimezone(ist).strftime("%Y-%m-%d %H:%M:%S")


YStartTime = get_current_ist_time()

customer_merchant_stats = joined_df.groupBy("merchant", "customer").agg(
    F.count("*").alias("customer_txn_count"),
    F.avg("amount").alias("customer_avg_value")
)

child_candidates = customer_merchant_stats.filter(
    (F.col("customer_avg_value") < 23) & (F.col("customer_txn_count") >= 80)
)

child_detections = []
for row in child_candidates.collect():
    detectionTime = get_current_ist_time()
    child_detections.append({
        "YStartTime": YStartTime,
        "detectionTime": detectionTime,
        "patternId": PATTERN_ID,
        "ActionType": ACTION_TYPE,
        "customerName": row["customer"] if row["customer"] is not None else "",
        "MerchantId": row["merchant"] if row["merchant"] is not None else ""
    })

print(f"CHILD detections found: {len(child_detections)}")
for det in child_detections[:5]:
    print(det)


if child_detections:
    child_pd = pd.DataFrame(child_detections)
    for i in range(0, len(child_pd), 50):
        batch = child_pd.iloc[i:i+50]
        batch_id = uuid.uuid4().hex
        batch_path = f"{OUTPUT_BASE_PATH}child_detections_{batch_id}.csv"
        batch_spark = spark.createDataFrame(batch)
        batch_spark.coalesce(1).write.mode("overwrite").option("header", True).csv(batch_path)
        print(f"Wrote {len(batch)} CHILD detections to {batch_path}")
else:
    print("No CHILD detections found.")

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd
import uuid
from datetime import datetime
import pytz


OUTPUT_BASE_PATH = "abfss://needed@btaze1idefcsa02.dfs.core.windows.net/chunks"  
PATTERN_ID = "PatId3"
ACTION_TYPE = "DEI-NEEDED"

def get_current_ist_time():
    utc_now = datetime.utcnow()
    ist = pytz.timezone('Asia/Kolkata')
    return utc_now.replace(tzinfo=pytz.utc).astimezone(ist).strftime("%Y-%m-%d %H:%M:%S")


YStartTime = get_current_ist_time()

merchant_gender = joined_df.groupBy("merchant", "gender").agg(
    F.countDistinct("customer").alias("num_customers")
)

merchant_gender_pivot = merchant_gender.groupBy("merchant").pivot("gender", ["M", "F"]).sum("num_customers").fillna(0)

dei_candidates = merchant_gender_pivot.filter(
    (F.col("F") > 100) & (F.col("F") < F.col("M"))
)

dei_detections = []
for row in dei_candidates.collect():
    detectionTime = get_current_ist_time()
    dei_detections.append({
        "YStartTime": YStartTime,
        "detectionTime": detectionTime,
        "patternId": PATTERN_ID,
        "ActionType": ACTION_TYPE,
        "customerName": "",
        "MerchantId": row["merchant"] if row["merchant"] is not None else ""
    })

print(f"DEI-NEEDED detections found: {len(dei_detections)}")
for det in dei_detections[:5]:
    print(det)


if dei_detections:
    dei_pd = pd.DataFrame(dei_detections)
    for i in range(0, len(dei_pd), 50):
        batch = dei_pd.iloc[i:i+50]
        batch_id = uuid.uuid4().hex
        batch_path = f"{OUTPUT_BASE_PATH}dei_needed_detections_{batch_id}.csv"
        batch_spark = spark.createDataFrame(batch)
        batch_spark.coalesce(1).write.mode("overwrite").option("header", True).csv(batch_path)
        print(f"Wrote {len(batch)} DEI-NEEDED detections to {batch_path}")
else:
    print("No DEI-NEEDED detections found.")