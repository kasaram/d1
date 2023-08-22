from pyspark.sql import SparkSession

def main(input_file, date_val, output_file):
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("AthenaQuery").getOrCreate()

        # Create a DataFrame from the CSV file
        df = spark.read.csv(input_file, header=True, inferSchema=True)

        # Create a temporary table from the DataFrame
        df.createOrReplaceTempView("athena_table_name")

        # Define the SQL query
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

        # Custom success message
        print("Spark job completed successfully!")

    except Exception as e:
        # Custom failure message
        print("An error occurred:", e)

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 4:
        print("Usage: code.py <input_file> <date_val> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    date_val = sys.argv[2]
    output_file = sys.argv[3]

    main(input_file, date_val, output_file)



from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 17),  # Modify with the actual start date
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spark_job_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["example"],
)

# Modify these values accordingly
EMR_IP_Address = "your_emr_ip_address"
ssh_key_path = "/usr/emr.pem"
input_file = "s3://file/pd_file.csv"
date_val = "12-03-2023"
output_file = "s3://file/output.csv"

# Define the ssh command
ssh_command = (
    f"ssh -o StrictHostKeyChecking=no -t -i {ssh_key_path} hadoop@{EMR_IP_Address} "
    f"spark-submit /home/hadoop/code.py {input_file} {date_val} {output_file}"
)

def on_success_callback(context):
    print("Task succeeded. Spark job completed successfully!")

def on_failure_callback(context):
    print("Task failed. Spark job failed. Check logs for details.")

run_spark_job = BashOperator(
    task_id="run_spark_job",
    bash_command=ssh_command,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
