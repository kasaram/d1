from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, max
from datetime import datetime

# Initialize a Spark session
spark = SparkSession.builder.appName("DataValidation").getOrCreate()

# Load the input CSV files
raw_interface_path = "s3://your-s3-bucket/raw_interface.csv"  # Replace with your S3 path
validated_records_path = "s3://your-s3-bucket/validated_records.csv"  # Replace with your S3 path

# Load the process.raw_interface table
raw_interface = spark.read.csv(raw_interface_path, header=True, inferSchema=True)

# Load the validated records
validated_records = spark.read.csv(validated_records_path, header=True, inferSchema=True)

# Define the schema for process.raw_interface_updated
raw_interface_updated_schema = raw_interface.schema

# Filter validated_records to include only necessary fields
validated_records_subset = validated_records.select(
    col("day_rk").alias("validated_day_rk"),
    col("cis_code").alias("validated_cis_code"),
    col("definitive_pd").alias("validated_definitive_pd")
)

# Find the maximum day_rk for each counterparty_id in process.raw_interface
max_day_rk_per_counterparty = raw_interface.groupBy("counterparty_id").agg(max("day_rk").alias("max_day_rk"))

# Join process.raw_interface_updated with validated_records_subset based on specified conditions
joined_data = raw_interface.join(
    validated_records_subset,
    (col("counterparty_id") == max_day_rk_per_counterparty["counterparty_id"]) &
    (col("day_rk") == max_day_rk_per_counterparty["max_day_rk"]) &
    (col("postcrm") == col("validated_definitive_pd")) &
    (col("precrm") == col("validated_definitive_pd")),
    "left"
)

# Save the joined data to Athena as process.raw_interface_updated
joined_data.write.format("parquet").mode("overwrite").saveAsTable("process.raw_interface_updated")

# Stop the Spark session
spark.stop()
