from datetime import datetime, timedelta
from pyspark.sql import SparkSession

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

@dag(
    schedule=None, 
    start_date=datetime(2024, 1, 1), 
    tags=[], 
    params={}, 
)
def test_spark():

    @task
    def fetch_data_with_spark(**kwargs):
        date = "2024-01-01"
        lot_id = 'ATWLOT-010124-0852-553-001'

        client = (
            SparkSession.Builder()
            .appName("data_analyzer")
            .master("local[1]")
            .config("spark.hive.metastore.uris", "thrift://hadoop-platform:9083")
            .config(
                "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
                2,
            )
            .config(
                "spark.sql.execution.arrow.pyspark.enabled",
                "true" if True else "false",
            )
            .enableHiveSupport()
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
        
    data = fetch_data_with_spark()

test_saprk_dag = test_spark()
