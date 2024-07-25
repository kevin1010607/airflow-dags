from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 25),
    'retries': 1,
}

dag = DAG(
    'test-k8s',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
)

start = EmptyOperator(task_id='run_this_first', dag=dag)

passing = KubernetesPodOperator(
    namespace='default',
    image="python:3.8",
    cmds=["python", "-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="passing-test",
    task_id="passing-task",
    get_logs=True,
    dag=dag,
)

failing = KubernetesPodOperator(
    namespace='default',
    image="ubuntu:16.04",
    cmds=["echo"],
    arguments=["hello world"],
    labels={"foo": "bar"},
    name="fail",
    task_id="failing-task",
    get_logs=True,
    dag=dag,
)

start >> [passing, failing]
