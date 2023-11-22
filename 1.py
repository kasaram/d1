from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType, DoubleType
from datetime import datetime

def initialize_spark():
    return SparkSession.builder.appName("DataValidation").getOrCreate()

def load_csv(spark, path, header=True, infer_schema=True):
    try:
        return spark.read.csv(path, header=header, inferSchema=infer_schema)
    except Exception as e:
        raise Exception(f"Error loading CSV from {path}: {str(e)}")

def format_date_udf():
    return udf(lambda date_str: datetime.strptime(date_str, "%d-%b-%Y").strftime("%d%m%Y") if date_str else None, StringType())

def validate_cis_code(cis_code):
    return cis_code is not None and cis_code.rlike("^[a-zA-Z0-9]+$")

def validate_definitive_pd(pd, mgs):
    lookup_row = lookup_df.filter(lookup_df["mgs"] == mgs).first()
    if lookup_row:
        lower_bound = lookup_row["lower_bound"]
        upper_bound = lookup_row["upper_bound"]
        return pd is not None and lower_bound <= pd <= upper_bound
    else:
        return False

def validate_definitive_pd_udf():
    return udf(validate_definitive_pd, DoubleType())

def save_to_csv(dataframe, path):
    try:
        dataframe.write.csv(path, header=True, mode="overwrite")
    except Exception as e:
        raise Exception(f"Error saving DataFrame to {path}: {str(e)}")

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
        spark = initialize_spark()

        # Load input files
        global lookup_df
        lookup_df = load_csv(spark, lookup_file_path, header=True, infer_schema=True)
        input_df = load_csv(spark, input_file_path, header=True, infer_schema=True)

        # Define UDF for date formatting
        date_format_udf = format_date_udf()

        # Apply validations
        validations = input_df.join(lookup_df, input_df["definitive_grade"] == lookup_df["mgs"], "left") \
            .withColumn("error", when(input_df["cis_code"].isNull(), "cis_code is empty")
                                  .when(~validate_cis_code(input_df["cis_code"]), "cis_code is not alphanumeric")
                                  .when((input_df["definitive_pd"] < 0) | (input_df["definitive_pd"] > 1), "definitive_pd is out of range")
                                  .when((input_df["definitive_grade"] < 1) | (input_df["definitive_grade"] > 27), "definitive_grade is out of range")
                                  .when(~input_df["cascade_flag"].isin("Y", "N"), "cascade_flag is invalid")
                                  .when(~validate_definitive_pd_udf()(input_df["definitive_pd"], input_df["definitive_grade"]), "definitive_pd is not within the range specified in lookup")
                                  .when((input_df["model_name"] == "Funds") & (input_df["segment"].isNull()), "segment cannot be null when model_name is 'Funds'")
                                  .when(input_df["definitive_pd"].isNull(), "definitive_pd is empty")
                                  .otherwise(None))

        # Split into correct and error records
        correct_records = validations.filter(validations["error"].isNull())

        # Save the correct records to a new file
        save_to_csv(correct_records, correct_output_path)

        # Filter records based on year_month
        year_month = '202309'
        validated_records = correct_records.filter(col("last_grading_date").cast("string").substr(5, 6) == year_month)

        # Save the validated records to a new file
        save_to_csv(validated_records, validated_records_output_path)

        # Load the process.raw_interface table
        raw_interface = load_csv(spark, raw_interface_path, header=True, infer_schema=True)

        # Find the maximum day_rk for each counterparty_id in process.raw_interface
        max_day_rk_per_counterparty = raw_interface.groupBy("counterparty_id").agg(max("day_rk").alias("max_day_rk"))

        # Join process.raw_interface_updated with validated_records_subset based on specified conditions
        joined_data = raw_interface.join(
            validated_records,
            (col("counterparty_id") == max_day_rk_per_counterparty["counterparty_id"]) &
            (col("day_rk") == max_day_rk_per_counterparty["max_day_rk"]) &
            (col("postcrm") == col("definitive_pd")) &
            (col("precrm") == col("definitive_pd")),
            "left"
        )

        # Save the joined data to Athena as process.raw_interface_updated
        joined_data.write.format("parquet").mode("overwrite").saveAsTable("process.raw_interface_updated")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    main()
