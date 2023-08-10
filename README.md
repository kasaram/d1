from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def my_task_function(**kwargs):
    parameter_date = kwargs['dag_run'].conf.get('date')
    parameter_path = kwargs['dag_run'].conf.get('path')
    # Use parameter_date and parameter_path in your task logic

with DAG('my_parameterized_dag', schedule_interval=None, start_date=days_ago(1)) as dag:
    my_task = PythonOperator(
        task_id='my_task',
        python_callable=my_task_function,
        provide_context=True,
    )



from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

@app.route('/trigger_dag', methods=['POST'])
def trigger_dag():
    parameter_date = request.json.get('date')
    parameter_path = request.json.get('path')
    
    # Trigger Airflow DAG using the parameters
    response = requests.post('http://airflow_webserver:8080/api/experimental/dags/my_parameterized_dag/dag_runs',
                             json={"conf": {"date": parameter_date, "path": parameter_path}})
    return jsonify({'message': 'DAG triggered successfully'})

if __name__ == '__main__':
    app.run(debug=True)




<!DOCTYPE html>
<html>
<head>
    <title>Trigger Airflow DAG</title>
</head>
<body>
    <input type="text" id="dateInput" placeholder="Enter Date (DDMMYYYY)">
    <input type="text" id="pathInput" placeholder="Enter Path (s3://input.csv)">
    <button id="triggerButton">Trigger DAG</button>
    <script>
        const triggerButton = document.getElementById('triggerButton');
        const dateInput = document.getElementById('dateInput');
        const pathInput = document.getElementById('pathInput');
        
        triggerButton.addEventListener('click', async () => {
            const dateValue = dateInput.value;
            const pathValue = pathInput.value;
            const response = await fetch('/trigger_dag', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ date: dateValue, path: pathValue })
            });
            const data = await response.json();
            alert(data.message);
        });
    </script>
</body>
</html>



----------

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 10),
    # Add other default arguments
}

with DAG('your_dag_id', default_args=default_args, schedule_interval=None) as dag:
    trigger_task = BashOperator(
        task_id='trigger_task',
        bash_command='your_script.sh {{ dag_run.conf["date"] }} {{ dag_run.conf["path"] }}',
    )



#!/bin/bash

date=$1
path=$2

python /path/to/your_script.py --date $date --path $path



from flask import Flask, request, jsonify
import requests
import re

app = Flask(__name__)

AIRFLOW_API_URL = "http://your_airflow_instance/api/experimental/dags/your_dag_id/dag_runs"

# Regular expression pattern to validate 'YYYYMMDD' date format
DATE_PATTERN = r'^\d{8}$'

# Function to validate path format
def is_valid_path(path):
    return path.startswith("s3://")  # You can adjust the validation logic as needed

@app.route('/trigger_airflow', methods=['POST'])
def trigger_airflow():
    data = request.json
    date = data.get('date')
    path = data.get('path')

    # Validate date format
    if not re.match(DATE_PATTERN, date):
        return jsonify({"error": "Invalid date format. Please use YYYYMMDD."}), 400

    # Validate path format
    if not is_valid_path(path):
        return jsonify({"error": "Invalid path format. Please use a valid path format."}), 400

    payload = {
        "conf": {
            "date": date,
            "path": path
        }
    }

    response = requests.post(AIRFLOW_API_URL, json=payload)
    return jsonify(response.json())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

