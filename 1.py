from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.window import Window

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
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from datetime import datetime

date_format_udf = udf(lambda date_str: datetime.strptime(date_str, "%d%m%Y").strftime("%d%m%Y")
                     if date_str else None, StringType())

# Define validation functions
def validate_cis_code(cis_code):
    return (cis_code is not None) and cis_code.isalnum()

def validate_definitive_pd(pd, mgs):
    lookup_row = lookup_df.filter(lookup_df["mgs"] == mgs).first()
    if lookup_row:
        lower_bound = lookup_row["lower_bound"]
        upper_bound = lookup_row["upper_bound"]
        return (pd is not None) and (lower_bound <= pd <= upper_bound)
    else:
        return False

def validate_record(row):
    errors = []

    # Validation 1
    if not row["cis_code"]:
        errors.append("cis_code is empty")

    # Validation 2
    if not (0 <= row["definitive_pd"] <= 1):
        errors.append("definitive_pd is out of range")

    # Validation 3
    if not (1 <= row["definitive_grade"] <= 27):
        errors.append("definitive_grade is out of range")

    # Validation 4
    if row["cascade_flag"] not in ("Y", "N"):
        errors.append("cascade_flag is invalid")

    # Validation 5
    if not validate_cis_code(row["cis_code"]):
        errors.append("cis_code is not alphanumeric")

    # Validation 6
    row["last_grading_date"] = date_format_udf(row["last_grading_date"])
    if not row["last_grading_date"]:
        errors.append("last_grading_date has an invalid format")

    # Validation 7
    if row["model_name"] == "Funds" and not row["segment"]:
        errors.append("segment cannot be null when model_name is 'Funds'")

    # Validation 8
    if not validate_definitive_pd(row["definitive_pd"], row["definitive_grade"]):
        errors.append("definitive_pd is not within the range specified in lookup")

    return row, ",".join(errors)

# Apply validations and split into correct and error records
validations = input_df.rdd.map(validate_record).toDF(["data", "error"])

correct_records = validations.filter(validations["error"] == "").select("data.*")
error_records = validations.filter(validations["error"] != "")

# Save the correct and error records to separate files
correct_records.write.csv(correct_output_path, header=True, mode="overwrite")
error_records.write.csv(error_output_path, header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
