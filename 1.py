from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("UpdateRawInterface").getOrCreate()

# Load raw_interface table from CSV file
raw_interface = spark.read.csv("/path/to/raw_interface.csv", header=True, inferSchema=True)

# Print the schema to identify the correct column names
print("Schema of raw_interface:")
raw_interface.printSchema()

# Step 1: Print the total number of records in raw_interface
total_records_before_update = raw_interface.count()
print(f'Total no of records in raw_interface now: {total_records_before_update}')

# Step 2: Get max_day = max(day_rk) and display max day_rk
max_day_rk = raw_interface.agg(F.max("day_rk").alias("max_day_rk")).first()["max_day_rk"]
print(f'Max day_rk is: {max_day_rk}')

# Step 3: Create raw_interface_max_day_rk_data
raw_interface_max_day_rk_data = raw_interface.filter(raw_interface["day_rk"] == max_day_rk)

# Step 4: Print the total number of records in raw_interface_max_day_rk_data
total_records_max_day_rk_data = raw_interface_max_day_rk_data.count()
print(f'Total no of records in raw_interface_max_day_rk_data: {total_records_max_day_rk_data}')

# Step 5: Update counterparty_id in raw_interface_max_day_rk_data
validated_records = spark.table("validated_records")  # Assuming validated_records is your DataFrame
raw_interface_max_day_rk_data = (
    raw_interface_max_day_rk_data
    .join(
        F.broadcast(validated_records),
        (raw_interface_max_day_rk_data["pd_score_postcrm"] == validated_records["definitive_pd"]) &
        (raw_interface_max_day_rk_data["pd_score_precrm"] == validated_records["definitive_pd"]),
        "left_outer"
    )
    .withColumn("counterparty_id", F.coalesce(validated_records["cis_code"], raw_interface_max_day_rk_data["counterparty_id"]))
)

# Step 6: Print count of raw_interface_max_day_rk_data after updating counterparty_id
total_records_updated_max_day_rk_data = raw_interface_max_day_rk_data.count()
print(f'Total no of records in raw_interface_max_day_rk_data after updating counterparty_id: {total_records_updated_max_day_rk_data}')

# Step 7: Display only records with modified counterparty_id in raw_interface_max_day_rk_data
modified_records = raw_interface_max_day_rk_data.filter("counterparty_id != updated_cis_code")
modified_records.show(truncate=False)

# Step 8: Print the total number of records in raw_interface
total_records_final = raw_interface.count()
print(f'Total no of records in raw_interface after update: {total_records_final}')

# Step 9: Compare raw_interface_final record count with raw_interface count from Step 1
if total_records_before_update == total_records_final:
    print("Counts match!")
else:
    print("Counts do not match!")

# Stop the Spark session
spark.stop()
