from datetime import datetime, timedelta

from airflow.decorators import dag, task

def error_callback(context):
    print(f"An error occurred: {context=}")

@dag(
    schedule=None, 
    start_date=datetime(2024, 1, 1), 
    tags=[], 
    params={}, 
)
def test_k8s():

    @task(on_failure_callback=error_callback)
    def extract():
        # Simulating data extraction
        raise Exception("Failed to extract data")
        return {"order_id": 1234, "amount": 100.00}

    @task.kubernetes(
        image="python:3.12", 
        namespace="airflow", 
        do_xcom_push=True, 
    )
    def transform(order_data: dict):
        import time
        # Simulating data transformation
        time.sleep(20)
        order_data['amount'] = order_data['amount'] * 1.1  # Add 10% tax
        return order_data

    @task()
    def load(order_data: dict):
        # Simulating data loading
        print(f"Saving order {order_data['order_id']} with amount {order_data['amount']}")

    # Define the task dependencies
    order_data = extract()
    transformed_data = transform(order_data)
    load(transformed_data)

# Instantiate the DAG
test_k8s_dag = test_k8s()
