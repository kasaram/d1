from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("AthenaQuery").getOrCreate()

# Input values
input_file = "s3://file/pd_file.csv"
date_val = "12-03-2023"
output_file = "s3://file/output.csv"

# Create a DataFrame from the CSV file
df = spark.read.csv(input_file, header=True, inferSchema=True)

# Create a temporary table from the DataFrame
df.createOrReplaceTempView("athena_table_name")

# Define the SQL query using format()
sql_query = """
    SELECT *
    FROM athena_table_name
    WHERE grading_date = '{}'
""".format(date_val)

# Execute the query
filtered_df = spark.sql(sql_query)

# Write the filtered DataFrame to a CSV file
filtered_df.write.csv(output_file, header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
