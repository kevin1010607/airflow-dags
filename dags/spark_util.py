# sql.py
from typing import List, Tuple

import pandas as pd
import numpy as np
from pyspark.sql import DataFrame, SparkSession

SQL_LIST = [
    """
SELECT * FROM ( SELECT * FROM ( SELECT mapping_uid, measure_item, value FROM qa WHERE (date = '2023-02-01' AND lot_id = 'ATWLOT-020123-0620-718-001') ) t PIVOT ( FIRST(value) FOR measure_item IN ('ball_shear', 'neck_pull', 'wire_pull', 'stitch_pull', 'outer_ball_size_x', 'outer_ball_size_y', 'inner_ball_size_x', 'inner_ball_size_y', 'outer_ball_shape', 'inner_ball_shape', 'ball_placement_x', 'ball_placement_y', 'al_squeeze_out_x', 'al_squeeze_out_y', 'ball_thickness', 'loop_height', 'anomaly_bb', 'anomaly_bs') ) ) t1 INNER JOIN ( SELECT * FROM motion_bb_mfdotwbdot000001 WHERE (date = '2023-02-01' AND lot_id = 'ATWLOT-020123-0620-718-001') ) t2 USING(mapping_uid)
""",
    """
SELECT * FROM ( SELECT * FROM ( SELECT mapping_uid, measure_item, value FROM qa WHERE (date = '2023-02-01' AND lot_id = 'ATWLOT-020123-0640-719-002') ) t PIVOT ( FIRST(value) FOR measure_item IN ('ball_shear', 'neck_pull', 'wire_pull', 'stitch_pull', 'outer_ball_size_x', 'outer_ball_size_y', 'inner_ball_size_x', 'inner_ball_size_y', 'outer_ball_shape', 'inner_ball_shape', 'ball_placement_x', 'ball_placement_y', 'al_squeeze_out_x', 'al_squeeze_out_y', 'ball_thickness', 'loop_height', 'anomaly_bb', 'anomaly_bs') ) ) t1 INNER JOIN ( SELECT * FROM motion_bb_mfdotwbdot000001 WHERE (date = '2023-02-01' AND lot_id = 'ATWLOT-020123-0640-719-002') ) t2 USING(mapping_uid)
""",
    """
SELECT * FROM ( SELECT * FROM ( SELECT mapping_uid, measure_item, value FROM qa WHERE (date = '2023-02-01' AND lot_id = 'ATWLOT-020123-0700-720-003') ) t PIVOT ( FIRST(value) FOR measure_item IN ('ball_shear', 'neck_pull', 'wire_pull', 'stitch_pull', 'outer_ball_size_x', 'outer_ball_size_y', 'inner_ball_size_x', 'inner_ball_size_y', 'outer_ball_shape', 'inner_ball_shape', 'ball_placement_x', 'ball_placement_y', 'al_squeeze_out_x', 'al_squeeze_out_y', 'ball_thickness', 'loop_height', 'anomaly_bb', 'anomaly_bs') ) ) t1 INNER JOIN ( SELECT * FROM motion_bb_mfdotwbdot000001 WHERE (date = '2023-02-01' AND lot_id = 'ATWLOT-020123-0700-720-003') ) t2 USING(mapping_uid)
""",
]

ID = "mapping_uid"
DATETIME = "datetime"
_scope_features = [
    "al_squeeze_out_x",
    "al_squeeze_out_y",
    "outer_ball_size_x",
    "outer_ball_size_y",
    "ball_thickness",
    "loop_height",
    "outer_ball_shape",
    "inner_ball_shape",
]
_gauge_features = ["ball_shear", "wire_pull", "neck_pull", "stitch_pull"]
Y_LABELS = _scope_features + _gauge_features
TEXT_FEATURES = [
    "recipe_name",
    "lf_id",
    "lot_id",
]
DATETIME_FEATURES = ["proc_datetime", "date"]


def getSparkClient(appName: str = "spark-client", simulate: bool = False):
    if simulate:
        return (
            SparkSession.builder.appName(appName)
            .master("local[*]")
            .getOrCreate()
        )
    spark = (
        SparkSession.builder.appName(appName)
        .config("spark.hive.metastore.uris", "thrift://hadoop-platform:9083")
        .enableHiveSupport()
        .master("spark://hadoop-platform:7077")
        .getOrCreate()
    )
    return spark


def getAll(client: SparkSession):
    """getAll gets all data from the sql list and returns a dataframe"""
    df = client.sql(SQL_LIST[0])
    for i in range(1, len(SQL_LIST)):
        df = df.union(client.sql(SQL_LIST[i]))
    return df


def getSplittedData(client: SparkSession) -> Tuple[DataFrame, DataFrame]:
    """getSplittedData gets the splitted data from the sql list and returns a tuple of dataframes"""
    train = client.sql(SQL_LIST[0])
    train = train.union(client.sql(SQL_LIST[1]))
    test = client.sql(SQL_LIST[2])
    return train, test


def getLabels(df: DataFrame) -> Tuple[List[str], List[str]]:
    """getXLabels gets the x labels from the sql list and returns a dataframe"""
    cols = df.columns
    x_labels = [col for col in cols if col not in [ID] + Y_LABELS]
    y_labels = [col for col in cols if col in Y_LABELS]
    return x_labels, y_labels


def saveAll(client: SparkSession, path: str = "data.csv"):
    """saveAll saves all data from the sql list to csv"""
    spark_df = getAll(client)
    df = pd.DataFrame(spark_df.collect(), columns=spark_df.columns)
    df.to_csv(path, index=False)


def saveSplittedData(
    client: SparkSession,
    train_path: str = "train_data.csv",
    test_path: str = "test_data.csv",
):
    """saveSplittedData saves the splitted data from the sql list to csv"""
    spark_train, spark_test = getSplittedData(client)
    train = pd.DataFrame(spark_train.collect(), columns=spark_train.columns)
    test = pd.DataFrame(spark_test.collect(), columns=spark_test.columns)
    train.to_csv(train_path, index=False)
    test.to_csv(test_path, index=False)


def loadAll(client: SparkSession, path: str = "data.csv"):
    """loadAll loads all data from the csv file"""
    df = pd.read_csv(path)

    df[DATETIME] = pd.to_datetime(df[DATETIME])
    for datetime in DATETIME_FEATURES:
        df[datetime] = pd.to_datetime(df[datetime])
    sdf = client.createDataFrame(df).replace(float('nan'), None)
    return sdf


def loadSplittedData(
    client: SparkSession,
    train_path: str = "train_data.csv",
    test_path: str = "test_data.csv",
):
    """loadSplittedData loads the splitted data from the csv file"""
    train_df = pd.read_csv(train_path)
    test_df = pd.read_csv(test_path)

    train_df[DATETIME] = pd.to_datetime(train_df[DATETIME])
    test_df[DATETIME] = pd.to_datetime(test_df[DATETIME])
    for datetime in DATETIME_FEATURES:
        train_df[datetime] = pd.to_datetime(train_df[datetime])
        test_df[datetime] = pd.to_datetime(test_df[datetime])
    train_sdf = client.createDataFrame(train_df).replace(float('nan'), None)
    test_sdf = client.createDataFrame(test_df).replace(float('nan'), None)
    return train_sdf, test_sdf
