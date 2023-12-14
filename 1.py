from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

# Initialize Spark session
spark = SparkSession.builder.appName("RawInterfaceProcessing").getOrCreate()

# Sample data for validated_records
validated_records_data = [
    (1, "cis1", "entity1", "model1", "v1", "seg1", "grade1", 0.2, "Y", "Country1", "2023-01-01"),
    (2, "cis2", "entity2", "model2", "v2", "seg2", "grade2", 0.3, "N", "Country2", "2023-01-02"),
    # Add more data as needed
]

# Sample data for raw_interface
raw_interface_data = [
    ("01-01-2023", "cp1", "postcrm1", "precrm1", "col1", "col2"),
    ("01-02-2023", "cp2", "postcrm2", "precrm2", "col1", "col2"),
    ("01-02-2023", "cp2", "postcrm2", "precrm2", "col1", "col2"),
    ("01-02-2023", "cp2", "postcrm2", "precrm2", "col1", "col2"),
    ("01-09-2023", "cp2", "postcrm2", "precrm2", "col1", "col2"),
    # Add more data as needed
]

# Create DataFrames
validated_records_df = spark.createDataFrame(validated_records_data, ["sl_no", "cis_code", "entity_name", "model_name", "model_version", "segment", "definitive_grade", "definitive_pd", "cascade_flag", "country_of_operations", "last_grading_date"])
raw_interface_df = spark.createDataFrame(raw_interface_data, ["day_rk", "counterparty_id", "pd_score_postcrm", "pd_score_precrm", "col1", "col2"])

# 1. Copy raw_interface to "raw_interface_new" temp dataframe
raw_interface_new_df = raw_interface_df.withColumnRenamed("counterparty_id", "cis_code")

# 2. Print total number of records in "raw_interface_new"
print(f'Total no of records in "raw_interface_new" now: {raw_interface_new_df.count()}')

# 3. Get max_day = max(day_rk) and display max day_rk
max_day = raw_interface_new_df.select(max(col("day_rk"))).first()[0]
print(f'Max day_rk is: {max_day}')

# 4. Create "raw_interface_new_max_day_rk_data" from "raw_interface_new" where day_rk = max_day
raw_interface_new_max_day_rk_data = raw_interface_new_df.filter(col("day_rk") == max_day)

# 5. Print total number of records in "raw_interface_new_max_day_rk_data"
print(f'Total no of records in "raw_interface_new_max_day_rk_data": {raw_interface_new_max_day_rk_data.count()}')

# 6. Create "raw_interface_new_non_max_day_rk_data" from "raw_interface_new" where day_rk not equal max_day
raw_interface_new_non_max_day_rk_data = raw_interface_new_df.filter(col("day_rk") != max_day)

# 7. Print total number of records in "raw_interface_new_non_max_day_rk_data"
print(f'Total no of records in "raw_interface_new_non_max_day_rk_data": {raw_interface_new_non_max_day_rk_data.count()}')

# 8. Display raw_interface_new_max_day_rk_data and raw_interface_new_non_max_day_rk_data records
print("Records in raw_interface_new_max_day_rk_data:")
raw_interface_new_max_day_rk_data.show(truncate=False)

print("Records in raw_interface_new_non_max_day_rk_data:")
raw_interface_new_non_max_day_rk_data.show(truncate=False)

# Stop Spark session
spark.stop()
