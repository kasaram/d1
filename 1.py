from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType, DoubleType
from datetime import datetime

def initialize_spark():
    return SparkSession.builder.appName("DataValidation").getOrCreate()

def load_csv(spark, path, header=True, infer_schema=True):
    return spark.read.csv(path, header=header, inferSchema=infer_schema)

def register_udfs(spark):
    spark.udf.register("format_date", lambda date_str: datetime.strptime(date_str, "%d-%b-%Y").strftime("%d%m%Y") if date_str else None, StringType())
    spark.udf.register("validate_cis_code", lambda cis_code: cis_code is not None and cis_code.rlike("^[a-zA-Z0-9]+$"))
    spark.udf.register("validate_definitive_pd", validate_definitive_pd)

def validate_definitive_pd(pd, mgs):
    lookup_row = spark.sql(f"SELECT * FROM lookup WHERE mgs = {mgs}").first()
    if lookup_row:
        lower_bound = lookup_row["lower_bound"]
        upper_bound = lookup_row["upper_bound"]
        return pd is not None and lower_bound <= pd <= upper_bound
    else:
        return False

def main():
    try:
        # Set your S3 paths
        input_file_path = "s3://your-s3-bucket/input_file.csv"
        lookup_file_path = "s3://your-s3-bucket/lookup.csv"
        correct_output_path = "s3://your-s3-bucket/correct_records.csv"
        error_output_path = "s3://your-s3-bucket/error_records.csv"
        validated_records_output_path = "s3://your-s3-bucket/validated_records.csv"
        raw_interface_path = "s3://your-s3-bucket/raw_interface.csv"

        # Load Spark session
        global spark
        spark = initialize_spark()

        # Load input files
        global lookup_df
        lookup_df = load_csv(spark, lookup_file_path, header=True, infer_schema=True)
        input_df = load_csv(spark, input_file_path, header=True, infer_schema=True)

        # Register UDFs
        register_udfs(spark)

        # Create temporary views for DataFrames
        lookup_df.createOrReplaceTempView("lookup")
        input_df.createOrReplaceTempView("input_data")

        # Apply validations using Spark SQL
        validations = spark.sql("""
            SELECT *,
                CASE
                    WHEN cis_code IS NULL THEN 'cis_code is empty'
                    WHEN NOT validate_cis_code(cis_code) THEN 'cis_code is not alphanumeric'
                    WHEN definitive_pd < 0 OR definitive_pd > 1 THEN 'definitive_pd is out of range'
                    WHEN definitive_grade < 1 OR definitive_grade > 27 THEN 'definitive_grade is out of range'
                    WHEN cascade_flag NOT IN ('Y', 'N') THEN 'cascade_flag is invalid'
                    WHEN NOT validate_definitive_pd(definitive_pd, definitive_grade) THEN 'definitive_pd is not within the range specified in lookup'
                    WHEN model_name = 'Funds' AND segment IS NULL THEN 'segment cannot be null when model_name is Funds'
                    WHEN definitive_pd IS NULL THEN 'definitive_pd is empty'
                    ELSE NULL
                END AS error
            FROM input_data
        """)

        # Split into correct and error records
        correct_records = validations.filter(validations["error"].isNull())

        # Save the correct records to a new file
        correct_records.write.csv(correct_output_path, header=True, mode="overwrite")

        # Filter records based on year_month using Spark SQL
        year_month = '202309'
        spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW validated_records AS SELECT * FROM correct_records WHERE substr(last_grading_date, 5, 6) = {year_month}")

        # Save the validated records to a new file
        validated_records = spark.sql("SELECT * FROM validated_records")
        validated_records.write.csv(validated_records_output_path, header=True, mode="overwrite")

        # Load the process.raw_interface table
        raw_interface = load_csv(spark, raw_interface_path, header=True, infer_schema=True)

        # Find the maximum day_rk for each counterparty_id in process.raw_interface using Spark SQL
        max_day_rk_per_counterparty = spark.sql("""
            SELECT counterparty_id, MAX(day_rk) AS max_day_rk
            FROM raw_interface
            GROUP BY counterparty_id
        """)

        # Join process.raw_interface_updated with validated_records_subset based on specified conditions using Spark SQL
        joined_data = spark.sql("""
            SELECT r.*, v.*
            FROM raw_interface r
            LEFT JOIN validated_records v
            ON r.counterparty_id = v.counterparty_id
            AND r.day_rk = v.max_day_rk
            AND r.postcrm = v.definitive_pd
            AND r.precrm = v.definitive_pd
        """)

        # Save the joined data to Athena as process.raw_interface_updated
        joined_data.write.format("parquet").mode("overwrite").saveAsTable("process.raw_interface_updated")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    main()
