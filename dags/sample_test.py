from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta



# Define the Python function
def print_hello():
    print("Hello from Airflow!")

# Instantiate the DAG
dag = DAG(
    'simple_python_operator_dag',
    description='A simple PythonOperator DAG',
    catchup=False,
)

# Define the task
hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Set the task in the DAG
hello_task
