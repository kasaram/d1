from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("UpdateRawInterface").getOrCreate()

# Load raw_interface table from CSV file
raw_interface = spark.read.csv("/path/to/raw_interface.csv", header=True, inferSchema=True)

# Step 1: Copy raw_interface to raw_interface_new with the same column names
raw_interface_new = raw_interface.withColumnRenamed("counterparty_id", "updated_cis_code")

# Step 2: Print the total number of records in raw_interface_new
total_records_before_update = raw_interface_new.count()
print(f'Total no of records in raw_interface_new now: {total_records_before_update}')

# Step 3: Get max_day = max(day_rk) and display max day_rk
max_day_rk = raw_interface_new.agg(F.max("day_rk").alias("max_day_rk")).first()["max_day_rk"]
print(f'Max day_rk is: {max_day_rk}')

# Step 4: Create raw_interface_new_max_day_rk_data
raw_interface_new_max_day_rk_data = raw_interface_new.filter(raw_interface_new["day_rk"] == max_day_rk)

# Step 5: Print the total number of records in raw_interface_new_max_day_rk_data
total_records_max_day_rk_data = raw_interface_new_max_day_rk_data.count()
print(f'Total no of records in raw_interface_new_max_day_rk_data: {total_records_max_day_rk_data}')

# Step 6: Create raw_interface_new_non_max_day_rk_data
raw_interface_new_non_max_day_rk_data = raw_interface_new.filter(raw_interface_new["day_rk"] != max_day_rk)

# Step 7: Print the total number of records in raw_interface_new_non_max_day_rk_data
total_records_non_max_day_rk_data = raw_interface_new_non_max_day_rk_data.count()
print(f'Total no of records in raw_interface_new_non_max_day_rk_data: {total_records_non_max_day_rk_data}')

# Step 8: Update counterparty_id in raw_interface_new_max_day_rk_data
validated_records = spark.table("validated_records")  # Assuming validated_records is your DataFrame
raw_interface_new_max_day_rk_data = (
    raw_interface_new_max_day_rk_data
    .join(
        F.broadcast(validated_records),
        (raw_interface_new_max_day_rk_data["pd_score_postcrm"] == validated_records["definitive_pd"]) &
        (raw_interface_new_max_day_rk_data["pd_score_precrm"] == validated_records["definitive_pd"]),
        "left_outer"
    )
    .withColumn("updated_cis_code", F.coalesce(validated_records["cis_code"], raw_interface_new_max_day_rk_data["updated_cis_code"]))
)

# Step 9: Print count of raw_interface_new_max_day_rk_data after updating counterparty_id
total_records_updated_max_day_rk_data = raw_interface_new_max_day_rk_data.count()
print(f'Total no of records in raw_interface_new_max_day_rk_data after updating counterparty_id: {total_records_updated_max_day_rk_data}')

# Step 10: Display only records with modified counterparty_id in raw_interface_new_max_day_rk_data
modified_records = raw_interface_new_max_day_rk_data.filter("updated_cis_code != counterparty_id")
modified_records.show(truncate=False)

# Step 11: Create raw_interface_final by appending raw_interface_new_max_day_rk_data and raw_interface_new_non_max_day_rk_data
raw_interface_final = raw_interface_new_max_day_rk_data.union(raw_interface_new_non_max_day_rk_data)

# Step 12: Print the total number of records in raw_interface_final
total_records_final = raw_interface_final.count()
print(f'Total no of records in raw_interface_final: {total_records_final}')

# Step 13: Compare raw_interface_final record count with raw_interface_new count from Step 3
if total_records_before_update == total_records_final:
    print("Counts match!")
else:
    print("Counts do not match!")

# Stop the Spark session
spark.stop()
