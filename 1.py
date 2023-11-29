from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("UpdateRawInterface").getOrCreate()

# Assuming correct_records and raw_interface are your DataFrames
# Adjust column names accordingly

# Assuming correct_records DataFrame
correct_records = spark.table("correct_records")

# Assuming raw_interface DataFrame
raw_interface = spark.table("raw_interface")

# Convert day_rk to date format for comparison
raw_interface = raw_interface.withColumn("day_rk_date", F.to_date("day_rk", "dd-MM-yyyy"))

# Find the maximum day_rk value for each counterparty_id
max_day_rk_window = Window.partitionBy("counterparty_id").orderBy(F.desc("day_rk_date"))
max_day_rk_df = raw_interface.withColumn("max_day_rk", F.row_number().over(max_day_rk_window)).filter("max_day_rk = 1")

# Join the correct_records DataFrame with max_day_rk_df based on conditions
updated_raw_interface = raw_interface.join(
    correct_records,
    (raw_interface["counterparty_id"] == correct_records["cis_code"]) &
    (raw_interface["postcrm"] == correct_records["definitive_pd"]) &
    (raw_interface["precrm"] == correct_records["definitive_pd"]) &
    (raw_interface["day_rk_date"] == correct_records["last_grading_date"]),
    "left_outer"
).selectExpr("raw_interface.*", "correct_records.cis_code as updated_cis_code")

# Drop temporary columns
updated_raw_interface = updated_raw_interface.drop("day_rk_date", "max_day_rk")

# Display the updated raw_interface
updated_raw_interface.show()

# Save the updated_raw_interface DataFrame to a new table or location
# updated_raw_interface.write.mode("overwrite").saveAsTable("process.raw_interface_updated")

# Stop the Spark session
spark.stop()
