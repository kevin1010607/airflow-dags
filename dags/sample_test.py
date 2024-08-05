from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta



# Define the Python function
def print_hello():
    print("Hello from Airflow!")

# Instantiate the DAG
dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 8, 24), catchup=False)

# Define the task
hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Set the task in the DAG
hello_task
