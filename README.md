from pyspark.sql import SparkSession, functions as F

def main(input_file_path, date_val, output_file_path):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("AthenaTableCreation") \
        .getOrCreate()

    # Read CSV file into a DataFrame
    df = spark.read.option("header", "true").csv(input_file_path)

    # Create Athena-compatible table
    table_name = "athena_table"
    df.createOrReplaceTempView(table_name)

    # Apply the filter on grading_date
    filtered_df = spark.sql(f"SELECT * FROM {table_name} WHERE grading_date = '{date_val}'")

    # Write the filtered data to a CSV file
    filtered_df.write.csv(output_file_path, header=True, mode="overwrite")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    # Replace with your S3 paths and date value
    input_file = "s3://file/pd_file.csv"
    date_value = "12-03-2023"
    output_file = "s3://file/output.csv"

    main(input_file, date_value, output_file)
