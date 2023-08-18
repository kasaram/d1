from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
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

def handle_completion(**kwargs):
    ti = kwargs['ti']
    command_output = ti.xcom_pull(task_ids='run_spark_job')
    
    if command_output is not None:
        if "Spark job completed successfully!" in command_output:
            print("Spark job completed successfully!")
        else:
            print("An error occurred during the Spark job execution:")
            print(command_output)  # Display the captured output
    
    else:
        print("No output captured from the Spark job.")

run_spark_job = BashOperator(
    task_id="run_spark_job",
    bash_command=(
        "spark-submit code.py "
        "{{ dag_run.conf['input_file'] }} "
        "{{ dag_run.conf['date_value'] }} "
        "{{ dag_run.conf['output_file'] }}"
    ),
    xcom_push=True,  # Capture the output of the command
    on_failure_callback=handle_completion,
    on_success_callback=handle_completion,
    dag=dag,
)

completion_task = PythonOperator(
    task_id="completion_task",
    python_callable=handle_completion,
    provide_context=True,
    dag=dag,
)

run_spark_job >> completion_task


POST /api/experimental/dags/spark_job_dag/dag_runs
{
    "conf": {
        "input_file": "s3://file/pd_file.csv",
        "date_value": "12-03-2023",
        "output_file": "s3://file/output.csv"
    }
}
