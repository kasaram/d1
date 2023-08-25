from pyspark.sql import SparkSession

def main(input_file, date_val, output_file):
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("AthenaQuery").getOrCreate()

        # Create a DataFrame from the CSV file
        df = spark.read.csv(input_file, header=True, inferSchema=True)

        # Create a temporary table from the DataFrame
        df.createOrReplaceTempView("athena_table_name")

        # Define the SQL query with "last_grading_date"
        sql_query = """
            SELECT *
            FROM athena_table_name
            WHERE last_grading_date = '{}'
        """.format(date_val)

        # Execute the query
        filtered_df = spark.sql(sql_query)

        # Get the number of records in filtered_df
        num_records = filtered_df.count()

        # Write the filtered DataFrame to a CSV file
        filtered_df.write.csv(output_file, header=True, mode="overwrite")

        # Stop the Spark session
        spark.stop()

        # Custom success message with the number of records
        print(f"Spark job completed successfully! Number of records: {num_records}")

        # Return the number of records for XCom
        return num_records

    except Exception as e:
        # Custom failure message
        print("An error occurred:", e)
        # Return None in case of failure
        return None

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 4:
        print("Usage: code.py <input_file> <date_val> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    date_val = sys.argv[2]
    output_file = sys.argv[3]

    num_records = main(input_file, date_val, output_file)
