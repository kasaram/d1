from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
raw_interface_max_day_rk_data_after_update = (
    raw_interface_max_day_rk_data.withColumn(
        "counterparty_id",
        F.when(
            (raw_interface_max_day_rk_data["pd_score_postcrm"] == correct_records["definitive_pd"]) &
            (raw_interface_max_day_rk_data["pd_score_precrm"] == correct_records["definitive_pd"]),
            correct_records["cis_code"]
        ).otherwise(raw_interface_max_day_rk_data["counterparty_id"])
    )
)

# Step 6: Print count of raw_interface_max_day_rk_data after updating counterparty_id
total_records_updated_max_day_rk_data = raw_interface_max_day_rk_data_after_update.count()
print(f'Total no of records in raw_interface_max_day_rk_data after updating counterparty_id: {total_records_updated_max_day_rk_data}')

# Step 7: Display only records with modified counterparty_id in raw_interface_max_day_rk_data
modified_records = raw_interface_max_day_rk_data_after_update.filter("counterparty_id != cis_code")
modified_records.show(truncate=False)

# Step 8: Create raw_interface_final by updating the original DataFrame
raw_interface_final = raw_interface.withColumn(
    "counterparty_id",
    F.when(
        (raw_interface["day_rk"] == max_day_rk) &
        (raw_interface["pd_score_postcrm"] == correct_records["definitive_pd"]) &
        (raw_interface["pd_score_precrm"] == correct_records["definitive_pd"]),
        correct_records["cis_code"]
    ).otherwise(raw_interface["counterparty_id"])
)

# Step 9: Print the total number of records in raw_interface_final
total_records_final = raw_interface_final.count()
print(f'Total no of records in raw_interface_final: {total_records_final}')

# Step 10: Compare raw_interface_final record count with raw_interface count from Step 1
if total_records_before_update == total_records_final:
    print("Counts match!")
else:
    print("Counts do not match!")

# Stop the Spark session
spark.stop()
