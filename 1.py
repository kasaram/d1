from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import FloatType

# Initialize Spark session
spark = SparkSession.builder.appName("ValidationJob").getOrCreate()

# Load input data
input_file_path = "s3://your-bucket-name/path/to/input_file.csv"
lookup_file_path = "s3://your-bucket-name/path/to/lookup.csv"

input_df = spark.read.option("header", True).csv(input_file_path)
lookup_df = spark.read.option("header", True).csv(lookup_file_path)

# Define validation rules
def validate_cis_code(cis_code):
    return col(cis_code).rlike("^[a-zA-Z0-9]+$")

def validate_last_grading_date(date_col):
    return when(col(date_col).rlike("^\d{8}$"), col(date_col)).otherwise(None)

def validate_lookup(definitive_grade, definitive_pd, lookup_df):
    return when(
        col(definitive_grade) == lookup_df.mgs,
        (col(definitive_pd).cast(FloatType()) >= lookup_df.lower_bound) & (col(definitive_pd).cast(FloatType()) <= lookup_df.upper_bound)
    ).otherwise(True)

# Apply validations and create error dataframe
error_df = input_df \
    .withColumn("error", when(col("cis_code").isNull() | (col("cis_code") == ""), "cis_code field is empty") \
                .when(col("definitive_pd").isNull() | (col("definitive_pd") == ""), "definitive_pd field is empty") \
                .when((col("definitive_pd") < 0) | (col("definitive_pd") > 1), "definitive_pd value not between 0 and 1") \
                .when((col("definitive_grade") < 1) | (col("definitive_grade") > 27), "definitive_grade value not between 1 and 27") \
                .when(~col("cascade_flag").isin(["Y", "N"]), "Invalid value for cascade_flag") \
                .when(~validate_cis_code("cis_code"), "cis_code is not alphanumeric") \
                .when(~validate_last_grading_date("last_grading_date").isNotNull(), "Invalid format for last_grading_date") \
                .when((col("model_name") == "Funds") & col("segment").isNull(), "segment value cannot be null for model_name 'Funds'") \
                .when(~validate_lookup("definitive_grade", "definitive_pd", lookup_df), "definitive_pd value not within the specified range in lookup")
               )

# Create output and error dataframes
output_df = input_df.join(error_df, "sl_no", "left_anti")
output_df.show()

error_df = error_df.select("sl_no", "cis_code", "enitity_name", "model_name", "model_version",
                           "segment", "definitive_grade", "definitive_pd", "cascade_flag",
                           "country_of_operations", "last_grading_date", "error")

error_df.show()








from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("ValidationJob").getOrCreate()

# Load input data
input_file_path = "s3://your-bucket-name/path/to/input_file.csv"
lookup_file_path = "s3://your-bucket-name/path/to/lookup.csv"

input_df = spark.read.option("header", True).csv(input_file_path)
lookup_df = spark.read.option("header", True).csv(lookup_file_path)

# Register DataFrames as temporary SQL tables
input_df.createOrReplaceTempView("input_table")
lookup_df.createOrReplaceTempView("lookup_table")

# Apply validations using Spark SQL
result_df = spark.sql("""
    SELECT *
    FROM input_table
    WHERE
        (cis_code IS NULL OR cis_code = '') OR
        (definitive_pd IS NULL OR definitive_pd = '') OR
        (definitive_pd < 0 OR definitive_pd > 1) OR
        (definitive_grade < 1 OR definitive_grade > 27) OR
        NOT cascade_flag IN ('Y', 'N') OR
        NOT regexp_like(cis_code, '^[a-zA-Z0-9]+$') OR
        (model_name = 'Funds' AND segment IS NULL) OR
        NOT regexp_like(last_grading_date, '^\d{8}$') OR
        NOT EXISTS (
            SELECT 1
            FROM lookup_table
            WHERE input_table.definitive_grade = lookup_table.mgs
            AND CAST(input_table.definitive_pd AS FLOAT) BETWEEN lookup_table.lower_bound AND lookup_table.upper_bound
        )
""")

# Create error DataFrame
error_df = result_df.select("sl_no", "cis_code", "enitity_name", "model_name", "model_version",
                            "segment", "definitive_grade", "definitive_pd", "cascade_flag",
                            "country_of_operations", "last_grading_date")

# Display results
result_df.show()
error_df.show()
