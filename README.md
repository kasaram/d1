from flask import Flask, request, jsonify
import requests
import boto3

app = Flask(__name__)

AIRFLOW_API_URL = "http://<AIRFLOW_SERVER>:<PORT>/api/experimental/dags/spark_job_dag/dag_runs"

@app.route('/trigger_airflow_dag', methods=['POST'])
def trigger_airflow_dag():
    try:
        input_data = request.get_json()
        input_file = input_data.get('input_file')
        date_value = input_data.get('date_value')
        output_file = input_data.get('output_file')
        
        if not all([input_file, date_value, output_file]):
            return jsonify({"error": "Missing required input parameters"}), 400
        
        response = requests.post(AIRFLOW_API_URL, json={
            "conf": {
                "input_file": input_file,
                "date_value": date_value,
                "output_file": output_file
            }
        })
        
        if response.status_code == 200:
            return jsonify({"message": "Airflow DAG triggered successfully"})
        else:
            exception_message = response.headers.get("exception_message")
            if exception_message:
                return jsonify({"error": exception_message}), response.status_code
            else:
                return jsonify({"error": "Failed to trigger Airflow DAG"}), response.status_code
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/check_output_file', methods=['GET'])
def check_output_file():
    try:
        s3_path = "s3://output/outfile.csv"
        
        s3_client = boto3.client('s3')
        try:
            response = s3_client.head_object(Bucket='<YOUR_BUCKET_NAME>', Key='output/outfile.csv')
            file_exists = True
        except:
            file_exists = False
        
        if file_exists:
            return jsonify({"message": "Output file exists and contains data"})
        else:
            return jsonify({"message": "Output file does not exist or is empty"})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)




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

def handle_completion(**kwargs):
    ti = kwargs['ti']
    command_output = ti.xcom_pull(task_ids='run_spark_job')
    
    if command_output is not None:
        if "Spark job completed successfully!" in command_output:
            print("Spark job completed successfully!")
        else:
            print("An error occurred during the Spark job execution:")
            print(command_output)  # Display the captured output
            ti.xcom_push(key="exception_message", value=command_output)  # Send the exception message

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

run_spark_job
