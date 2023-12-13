from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("UpdateRawInterface").getOrCreate()

# Mockup data for raw_interface
raw_interface_schema = StructType([
    StructField("day_rk", StringType(), True),
    StructField("counterparty_id", StringType(), True),
    StructField("pd_score_postcrm", StringType(), True),
    StructField("pd_score_precrm", StringType(), True),
    StructField("col1", StringType(), True),
    StructField("col2", StringType(), True),
    # Add more dummy columns as needed
])

raw_interface_data = [
    ("2023-01-01", "CIS001", "PD1", "PD1", "dummy1", "dummy2"),
    ("2023-01-02", "CIS002", "PD2", "PD2", "dummy3", "dummy4"),
    # Add more rows as needed
]

raw_interface = spark.createDataFrame(raw_interface_data, schema=raw_interface_schema)

# Mockup data for correct_records
correct_records_schema = StructType([
    StructField("sl_no", StringType(), True),
    StructField("cis_code", StringType(), True),
    StructField("enitity_name", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("model_version", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("definitive_grade", StringType(), True),
    StructField("definitive_pd", StringType(), True),
    StructField("cascade_flag", StringType(), True),
    StructField("country_of_operations", StringType(), True),
    StructField("last_grading_date", StringType(), True),
])

correct_records_data = [
    ("1", "CIS001", "Entity1", "ModelA", "v1", "SegmentA", "GradeA", "PD1", "N", "CountryA", "2023-01-01"),
    ("2", "CIS002", "Entity2", "ModelB", "v2", "SegmentB", "GradeB", "PD2", "N", "CountryB", "2023-01-02"),
    # Add more rows as needed
]

correct_records = spark.createDataFrame(correct_records_data, schema=correct_records_schema)

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
print("Modified Records:")
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
