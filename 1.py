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

# Define the max_day_rk_window
max_day_rk_window = Window.partitionBy("counterparty_id").orderBy(F.desc("day_rk_date"))

# Find the maximum day_rk value for each counterparty_id
max_day_rk_df = (
    raw_interface
    .withColumn("max_day_rk", F.row_number().over(max_day_rk_window))
    .filter("max_day_rk = 1")
    .select("counterparty_id", "day_rk", "postcrm", "precrm", "max_day_rk")
)

# Join the correct_records DataFrame with max_day_rk_df based on conditions
updated_raw_interface = (
    raw_interface
    .join(
        F.broadcast(correct_records),
        (raw_interface["counterparty_id"] == correct_records["cis_code"]) &
        (raw_interface["postcrm"] == correct_records["definitive_pd"]) &
        (raw_interface["precrm"] == correct_records["definitive_pd"]) &
        (raw_interface["day_rk_date"] == correct_records["last_grading_date"]),
        "left_outer"
    )
    .join(
        max_day_rk_df,
        (raw_interface["counterparty_id"] == max_day_rk_df["counterparty_id"]) &
        (raw_interface["day_rk"] == max_day_rk_df["day_rk"]) &
        (raw_interface["postcrm"] == max_day_rk_df["postcrm"]) &
        (raw_interface["precrm"] == max_day_rk_df["precrm"]),
        "left_semi"
    )
    .select(
        raw_interface["*"],
        F.coalesce(correct_records["cis_code"], raw_interface["counterparty_id"]).alias("updated_cis_code"),
        F.when(correct_records["cis_code"].isNotNull(), "Updated").otherwise("Not Updated").alias("update_status")
    )
)

# Drop temporary columns
updated_raw_interface = updated_raw_interface.drop("day_rk_date", "max_day_rk")

# Display the updated and non-updated records separately
updated_raw_interface.show()

# Display only the modified records at the end
modified_records = updated_raw_interface.filter("update_status = 'Updated'")
print("\nBelow are modified rows in raw_interface table:")
modified_records.show(truncate=False)

# Display summary of modified records
summary_df = updated_raw_interface.groupBy("update_status").agg(F.count("update_status").alias("count"))
summary_df.show()

# Stop the Spark session
spark.stop()
