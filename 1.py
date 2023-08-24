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

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 4:
        print("Usage: code.py <input_file> <date_val> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    date_val = sys.argv[2]
    output_file = sys.argv[3]

    num_records = main(input_file, date_val, output_file)


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import XCom
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
MASTER_IP = "your_emr_master_ip"
KEY_FILE = "/path/to/your/key_file.pem"
input_file = "s3://file/pd_file.csv"
date_val = "12-03-2023"
output_file = "s3://file/output.csv"

def on_success_callback(context):
    num_records = context['task_instance'].xcom_pull(task_ids='run_spark_job')
    print(f"Task succeeded. Spark job completed successfully with {num_records} records!")

def on_failure_callback(context):
    print("Task failed. Spark job failed. Check logs for details.")

run_spark_job = BashOperator(
    task_id="run_spark_job",
    bash_command=ssh_command,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

def get_num_records(**kwargs):
    ti = kwargs['ti']
    num_records = ti.xcom_pull(task_ids='run_spark_job')
    return num_records

get_records_task = PythonOperator(
    task_id='get_records_task',
    python_callable=get_num_records,
    provide_context=True,
    dag=dag,
)

run_spark_job >> get_records_task

if __name__ == "__main__":
    dag.cli()
