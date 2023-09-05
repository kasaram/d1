from pyspark.sql.functions import lit

# ... (previous code) ...

# Apply validations and split into correct and error records
validations = input_df.rdd.map(validate_record)

correct_records = validations.filter(lambda x: x[1] == "").map(lambda x: x[0])
error_records = validations.filter(lambda x: x[1] != "").map(lambda x: x[0])

# Create a DataFrame with a column for error messages
error_records = error_records.map(lambda x: x + (lit(x[1]),)).toDF(["sl_no", "cis_code", "enitity_name", "model_name", "model_version", "segment", "definitive_grade", "definitive_pd", "cascade_flag", "country_of_operations", "last_grading_date", "error"])

# Save the correct and error records to separate files
correct_records.write.csv(correct_output_path, header=True, mode="overwrite")
error_records.write.csv(error_output_path, header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
