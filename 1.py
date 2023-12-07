from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("UpdateRawInterface").getOrCreate()

# Assuming validated_records and raw_interface are your DataFrames
# Adjust column names accordingly

# Assuming validated_records DataFrame
validated_records = spark.table("validated_records")

# Assuming raw_interface DataFrame
raw_interface = spark.table("raw_interface")

# Step 1: Copy raw_interface to raw_interface_new with the same column names
raw_interface_new = raw_interface.withColumnRenamed("counterparty_id", "updated_cis_code")

# Step 2: Print the total number of records in raw_interface_new
total_records_before_update = raw_interface_new.count()
print(f'Total no of records in raw_interface_new while creating: {total_records_before_update}')

# Step 3: Update raw_interface_new dataframe
max_day_rk_window = Window.partitionBy("updated_cis_code").orderBy(F.desc("day_rk"))

raw_interface_new = (
    raw_interface_new
    .withColumn("max_day_rk", F.row_number().over(max_day_rk_window))
    .filter("max_day_rk = 1")
    .join(
        F.broadcast(validated_records),
        (raw_interface_new["pd_score_postcrm"] == validated_records["definitive_pd"]) &
        (raw_interface_new["pd_score_precrm"] == validated_records["definitive_pd"]) &
        (raw_interface_new["day_rk"] == validated_records["last_grading_date"]),
        "left_outer"
    )
    .withColumn("updated_cis_code", F.coalesce(validated_records["cis_code"], raw_interface_new["updated_cis_code"]))
    .select(
        raw_interface_new["*"],
        F.when(validated_records["cis_code"].isNotNull(), 1).otherwise(0).alias("update_flag")
    )
)

# Step 4: Display the total number of rows updated
total_updated_records = raw_interface_new.filter("update_flag = 1").count()
print(f'Total no of rows updated with new cis_code: {total_updated_records}')

# Step 5: Display the total number of records in raw_interface_new after update
total_records_after_update = raw_interface_new.count()
print(f'No of records in raw_interface_new after update: {total_records_after_update}')

# Stop the Spark session
spark.stop()
