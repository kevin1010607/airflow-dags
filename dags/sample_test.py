from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_data_fetch',
    default_args=default_args,
    description='A DAG to fetch data using PySpark',
    schedule_interval=timedelta(days=1),
)

def fetch_data_with_spark(**kwargs):
    date = "2024-01-01"
    lot_id = 'ATWLOT-010124-0852-553-001'  # Replace with actual lot_id or pass it as a parameter

    # Replace these IP addresses with your actual server IPs
    spark_master_ip = "10.121.252.198"  # Example IP, replace with your Spark master IP
    hive_metastore_ip = "10.121.252.198"  # Example IP, replace with your Hive metastore IP

    client = (
        SparkSession.builder.appName("data_analyzer")
        .config("spark.hive.metastore.uris", f"thrift://{hive_metastore_ip}:9083")
        .enableHiveSupport()
        .master(f"spark://{spark_master_ip}:7077")
        .getOrCreate()
    )

    try:
        sql_str = f"""
        SELECT * FROM 
        (
            SELECT * FROM 
            (
                SELECT mapping_uid, measure_item, value 
                FROM qa 
                WHERE (date = '{date}' AND lot_id = '{lot_id}')
            ) t 
            PIVOT 
            (
                FIRST(value) FOR measure_item IN 
                ('ball_shear', 'neck_pull', 'wire_pull', 'stitch_pull', 'outer_ball_size_x', 
                'outer_ball_size_y', 'inner_ball_size_x', 'inner_ball_size_y', 'outer_ball_shape', 
                'inner_ball_shape', 'ball_placement_x', 'ball_placement_y', 'al_squeeze_out_x', 
                'al_squeeze_out_y', 'ball_thickness', 'loop_height', 'anomaly_bb', 'anomaly_bs')
            )
        ) t1 
        INNER JOIN 
        (
            SELECT * FROM motion_bb_mfdotwbdot000001 
            WHERE (date = '{date}' AND lot_id = '{lot_id}')
        ) t2 
        USING(mapping_uid)
        """

        df = client.sql(sql_str)
        pdf = df.toPandas()

        # Here you can process the pandas DataFrame as needed
        # For example, you could save it to a file or push it to another database
        # For this example, we'll just print the shape of the DataFrame
        print(f"Data shape: {pdf.shape}")

        # You might want to return some metadata about the operation
        return {
            'rows_fetched': pdf.shape[0],
            'columns_fetched': pdf.shape[1],
            'date': date,
            'lot_id': lot_id
        }

    finally:
        client.stop()

fetch_data_task = PythonOperator(
    task_id='fetch_data_with_spark',
    python_callable=fetch_data_with_spark,
    provide_context=True,
    dag=dag,
)

fetch_data_task 