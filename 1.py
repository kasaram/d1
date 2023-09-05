from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType
from datetime import datetime

# Initialize a Spark session
spark = SparkSession.builder.appName("DataValidation").getOrCreate()

# Load the input CSV file
input_file_path = "s3://your-s3-bucket/input_file.csv"  # Replace with your S3 path
lookup_file_path = "s3://your-s3-bucket/lookup.csv"      # Replace with your S3 path
correct_output_path = "s3://your-s3-bucket/correct_records.csv"  # Replace with your S3 path
error_output_path = "s3://your-s3-bucket/error_records.csv"      # Replace with your S3 path

input_df = spark.read.csv(input_file_path, header=True, inferSchema=True)
lookup_df = spark.read.csv(lookup_file_path, header=True, inferSchema=True)

# Define UDF for date formatting
date_format_udf = udf(lambda date_str: datetime.strptime(date_str, "%d%m%Y").strftime("%d%m%Y")
                     if date_str else None, StringType())

# Define validation functions
def validate_cis_code(cis_code):
    return (cis_code is not None) and cis_code.isalnum()

# Define validation UDFs
validate_cis_code_udf = udf(validate_cis_code)
validate_definitive_pd_udf = udf(validate_definitive_pd, DoubleType())

# Apply validations
validations = input_df.join(lookup_df, input_df["definitive_grade"] == lookup_df["mgs"], "left") \
    .withColumn("error", when(input_df["cis_code"].isNull(), "cis_code is empty")
                          .when(~input_df["cis_code"].rlike("^[a-zA-Z0-9]+$"), "cis_code is not alphanumeric")
                          .when((input_df["definitive_pd"] < 0) | (input_df["definitive_pd"] > 1), "definitive_pd is out of range")
                          .when((input_df["definitive_grade"] < 1) | (input_df["definitive_grade"] > 27), "definitive_grade is out of range")
                          .when(~input_df["cascade_flag"].isin("Y", "N"), "cascade_flag is invalid")
                          .when((input_df["last_grading_date"].cast("string").cast("date") == "1900-01-01") | (input_df["last_grading_date"].cast("string").cast("date").isNull()), "last_grading_date has an invalid format")
                          .when((input_df["model_name"] == "Funds") & (input_df["segment"].isNull()), "segment cannot be null when model_name is 'Funds'")
                          .when(~validate_cis_code_udf(input_df["cis_code"]), "cis_code is not alphanumeric")
                          .when(input_df["definitive_pd"].isNull(), "definitive_pd is empty")
                          .when((lookup_df["mgs"].isNull()) | ((input_df["definitive_pd"] < lookup_df["lower_bound"]) | (input_df["definitive_pd"] > lookup_df["upper_bound"])), "definitive_pd is not within the range specified in lookup")
                          .otherwise(None))

# Split into correct and error records
correct_records = validations.filter(validations["error"].isNull())
error_records = validations.filter(validations["error"].isNotNull())

# Save the correct and error records to separate files
correct_records.write.csv(correct_output_path, header=True, mode="overwrite")
error_records.write.csv(error_output_path, header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
