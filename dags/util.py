import base64
from datetime import datetime, timedelta
import os
import shutil
import string
import mlflow
import numpy as np
import pandas as pd
from typing import Tuple, List, Dict, Any
import requests
from catboost import CatBoostClassifier, CatBoostRegressor
from sklearn.multioutput import MultiOutputClassifier, MultiOutputRegressor
from hdfs import InsecureClient
from pyspark.sql import DataFrame,SparkSession
from pyspark.sql import functions as F

DEBUG = True
NODE_IP = os.environ['NODE_IP']
ALERT_SERVER_ENDPOINT = f'http://{NODE_IP}:30888'
MODEL_SERVICE_ENDPOINT = f'http://{NODE_IP}:30002'


def generate_mock_data(n_samples=200) -> pd.DataFrame:
    """Generate mock data for equipment and process variables.

    Args:
        n_samples (int): Number of samples to generate.

    Returns:
        pd.DataFrame: Mock data in pandas DataFrame.
    """
    np.random.seed(42)

    equipment_ids = ['EQ' + str(i).zfill(3) for i in range(1, 6)]
    lf_ids = ['LF' + str(i).zfill(3) for i in range(1, 4)]

    data = {
        'equipment_id': np.random.choice(equipment_ids, n_samples),
        'lf_id': np.random.choice(lf_ids, n_samples),
        'proc_datetime': [datetime(2024, 1, 1) + timedelta(minutes=i) for i in range(n_samples)],
        'heat_pre': np.random.uniform(100, 200, n_samples),
        'al_squeeze_out_x': np.random.normal(10, 2, n_samples),
        'al_squeeze_out_y': np.random.normal(15, 3, n_samples),
        'outer_ball_size_x': np.random.normal(50, 5, n_samples),
        'outer_ball_size_y': np.random.normal(55, 6, n_samples),
        'ball_thickness': np.random.normal(20, 2, n_samples),
        'loop_height': np.random.normal(30, 3, n_samples),
        'outer_ball_shape': np.random.uniform(0.8, 1.2, n_samples),
        'inner_ball_shape': np.random.uniform(0.9, 1.1, n_samples)
    }

    return pd.DataFrame(data)


def _DC(data: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with missing values from the dataframe.

    Args:
        data (pd.DataFrame): The input dataframe.

    Returns:
        pd.DataFrame: The dataframe with missing values dropped.
    """
    return data.dropna()


def _FE(
    data: pd.DataFrame,
    features_name: List[str] = [],
    targets_name: List[str] = []
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extract features and target variables from the dataframe.

    Args:
        data (pd.DataFrame): The input dataframe.
        features_name (List[str]): List of feature column names.
        targets_name (List[str]): List of target column names.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the features and targets dataframes.
    """
    feature = data[features_name]
    target = data[targets_name]

    return feature, target


def determine_data_type(y: pd.DataFrame) -> str:
    """Determine the type of the target variable (y).

    Args:
        y (pd.DataFrame): Target variables dataframe.

    Returns:
        str: The type of the target (classification/regression).
    """
    if y.shape[1] > 1:
        return "multi_y_classification" if np.issubdtype(y.dtype, np.integer) else "multi_y_regression"
    else:
        return "single_y_classification" if np.issubdtype(y.dtype, np.integer) else "single_y_regression"


def _GET_MODEL(y: pd.DataFrame):
    """Get an appropriate model based on the type of target variable.

    Args:
        y (pd.DataFrame): Target variables dataframe.

    Returns:
        model: An appropriate machine learning model.
    """
    data_type = determine_data_type(y)
    model = None

    if data_type == "single_y_classification":
        model = CatBoostClassifier(iterations=1000, learning_rate=0.1, depth=6)
    elif data_type == "multi_y_classification":
        base_model = CatBoostClassifier(iterations=1000, learning_rate=0.1, depth=6)
        model = MultiOutputClassifier(base_model)
    elif data_type == "single_y_regression":
        model = CatBoostRegressor(iterations=1000, learning_rate=0.1, depth=6)
    elif data_type == "multi_y_regression":
        base_model = CatBoostRegressor(iterations=1000, learning_rate=0.1, depth=6)
        model = MultiOutputRegressor(base_model)

    return model


def _LOAD_MODEL(model_name: str, model_version: str):
    """Load a model from the server.

    Args:
        model_name (str): Name of the model.
        model_version (str): Version of the model.

    Returns:
        model: The loaded model.
    """
    url = f'{MODEL_SERVICE_ENDPOINT}/load_model'
    data = {"model_name": model_name, "model_version": model_version}
    response = requests.post(url, json=data)
    result = response.json()["result"]

    model_path = "model"
    if os.path.exists(model_path):
        shutil.rmtree(model_path)

    os.mkdir(model_path)

    # Decode and save files
    files = ['conda.yaml', 'MLmodel', 'python_env.yaml', 'python_model.pkl', 'requirements.txt']
    contents = [base64.b64decode(result[key]) for key in ['conda', 'MLmodel', 'python_env', 'python_model', 'requirements']]

    for file, content in zip(files, contents):
        with open(os.path.join(model_path, file), 'wb') as f:
            f.write(content)

    loaded_model = mlflow.pyfunc.load_model(model_path)
    unwrapped_model = loaded_model.unwrap_python_model()

    if os.path.exists(model_path):
        shutil.rmtree(model_path)

    return unwrapped_model.model


def _LOG_METRIC(model_id: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Log a performance metric for a model to the server.

    Args:
        model_id (str): The ID of the model.
        metrics (Dict[str, Any]): Dictionary containing metric key and value.

    Returns:
        Dict[str, Any]: The response from the server.
    """
    url = f'{MODEL_SERVICE_ENDPOINT}/log_metrics'
    payload = {'model_name': model_id, 'metrics': metrics}
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as err:
        return {'state': False, 'message': str(err)}


def _LOG_PARAMETER(model_id: str, parameter: Dict[str, Any]) -> Dict[str, Any]:
    """Log a parameter for a model to the server.

    Args:
        model_id (str): The ID of the model.
        parameter (Dict[str, Any]): Dictionary containing parameter key and value.

    Returns:
        Dict[str, Any]: The response from the server.
    """
    parameter['model_status'] = 'normal'
    url = f'{MODEL_SERVICE_ENDPOINT}/log_parameters'
    payload = {'model_name': model_id, 'parameters': parameter}
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as err:
        return {'state': False, 'message': str(err)}


def _GET_MOTION_AND_QA(date: str, lot_id: str) -> pd.DataFrame:
    """Retrieve motion and QA data based on date and lot_id.

    Args:
        date (str): The date for which to retrieve data.
        lot_id (str): The lot ID for which to retrieve data.

    Returns:
        pd.DataFrame: A pandas dataframe containing the retrieved data.
    """
    client = (
        SparkSession.builder
        .appName("data_analyzer")
        .master("local[1]")
        .config("spark.hive.metastore.uris", "thrift://hadoop-platform:9083")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )

    sql_str = \
    f"SELECT * FROM ( SELECT * FROM ( SELECT mapping_uid, measure_item, value FROM qa WHERE (date = '{date}' AND lot_id = '{lot_id}') ) t PIVOT ( FIRST(value) FOR measure_item IN ('ball_shear', 'neck_pull', 'wire_pull', 'stitch_pull', 'outer_ball_size_x', 'outer_ball_size_y', 'inner_ball_size_x', 'inner_ball_size_y', 'outer_ball_shape', 'inner_ball_shape', 'ball_placement_x', 'ball_placement_y', 'al_squeeze_out_x', 'al_squeeze_out_y', 'ball_thickness', 'loop_height', 'anomaly_bb', 'anomaly_bs') ) ) t1 INNER JOIN ( SELECT * FROM motion_bb_mfdotwbdot000001 WHERE (date = '{date}' AND lot_id = '{lot_id}') ) t2 USING(mapping_uid)"


    df = client.sql(sql_str)
    pdf = df.toPandas()
    client.stop()

    return pdf


def _GET_MOTION(date: str, lot_id: str) -> pd.DataFrame:
    """Retrieve motion data based on date and lot ID.

    Args:
        date (str): The date for which to retrieve motion data.
        lot_id (str): The lot ID for which to retrieve motion data.

    Returns:
        pd.DataFrame: A pandas dataframe containing the retrieved motion data.
    """
    client = (
        SparkSession.Builder()
        .appName("data_analyzer")
        .master("local[1]")
        .config("spark.hive.metastore.uris", "thrift://hadoop-platform:9083")
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2,
        )
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )

    motion_sql_str = \
    f"SELECT * FROM motion_bb_mfdotwbdot000001 WHERE (date = '{date}' AND lot_id = '{lot_id}') LIMIT 100"

    df = client.sql(motion_sql_str)
    pdf = df.toPandas()
    client.stop()

    return pdf


def _GET_QA(date: str, lot_id: str) -> pd.DataFrame:
    """Retrieve QA data based on date and lot ID.

    Args:
        date (str): The date for which to retrieve QA data.
        lot_id (str): The lot ID for which to retrieve QA data.

    Returns:
        pd.DataFrame: A pandas dataframe containing the retrieved QA data.
    """
    client = (
        SparkSession.Builder()
        .appName("data_analyzer")
        .master("local[1]")
        .config("spark.hive.metastore.uris", "thrift://hadoop-platform:9083")
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2,
        )
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )

    qa_sql_str = \
    f"SELECT * FROM ( SELECT * FROM ( SELECT mapping_uid, measure_item, value FROM qa WHERE (date = '{date}' AND lot_id = '{lot_id}') ) t PIVOT ( FIRST(value) FOR measure_item IN ('ball_shear', 'neck_pull', 'wire_pull', 'stitch_pull', 'outer_ball_size_x', 'outer_ball_size_y', 'inner_ball_size_x', 'inner_ball_size_y', 'outer_ball_shape', 'inner_ball_shape', 'ball_placement_x', 'ball_placement_y', 'al_squeeze_out_x', 'al_squeeze_out_y', 'ball_thickness', 'loop_height', 'anomaly_bb', 'anomaly_bs') ) ) t1 INNER JOIN ( SELECT mapping_uid FROM motion_bb_mfdotwbdot000001 WHERE (date = '{date}' AND lot_id = '{lot_id}') ) t2 USING(mapping_uid)"

    df = client.sql(qa_sql_str)
    pdf = df.toPandas()
    client.stop()

    return pdf


def _ONEHOT_ENCODING(df: pd.DataFrame) -> pd.DataFrame:
    """Apply one-hot encoding to string columns in the dataframe.

    Args:
        df (pd.DataFrame): Input dataframe.

    Returns:
        pd.DataFrame: Dataframe with one-hot encoding applied.
    """
    string_columns = [
        col for col in df.columns if df[col].apply(lambda x: isinstance(x, str)).all()
    ]

    if len(string_columns) == 0:
        return df
    else:
        one_hot_encoded = pd.get_dummies(df[string_columns])
        df_encoded = pd.concat([df, one_hot_encoded], axis=1)
        df_encoded = df_encoded.drop(string_columns, axis=1)
        return df_encoded


def _EXTRACT_DATETIME(df: pd.DataFrame) -> pd.DataFrame:
    """Extract datetime components from datetime columns.

    Args:
        df (pd.DataFrame): Input dataframe.

    Returns:
        pd.DataFrame: Dataframe with extracted datetime components.
    """
    datetime_cols = df.select_dtypes(include=['datetime64', 'datetime64[ns]']).columns

    for datetime_col in datetime_cols:
        df[datetime_col] = pd.to_datetime(df[datetime_col])
        df[f"{datetime_col}_year"] = df[datetime_col].dt.year
        df[f"{datetime_col}_month"] = df[datetime_col].dt.month
        df[f"{datetime_col}_day"] = df[datetime_col].dt.day
        df[f"{datetime_col}_hour"] = df[datetime_col].dt.hour
        df[f"{datetime_col}_minute"] = df[datetime_col].dt.minute
        df[f"{datetime_col}_second"] = df[datetime_col].dt.second
        df = df.drop(datetime_col, axis=1)

    return df


def _LOG_MODEL(model_name: str, model) -> Dict[str, Any]:
    """Log the machine learning model to the server.

    Args:
        model_name (str): Name of the model.
        model: Model object to log.

    Returns:
        Dict[str, Any]: Server response.
    """
    class PyfuncWrapper(mlflow.pyfunc.PythonModel):
        def __init__(self, model):
            self.model = model

        def predict(self, context, model_input):
            return self.model.predict(model_input)

    model_path = "pyfunc_model"
    if os.path.exists(model_path):
        shutil.rmtree(model_path)

    mlflow.pyfunc.save_model(
        path=model_path,
        python_model=PyfuncWrapper(model),
    )

    files = ["conda.yaml", "MLmodel", "python_env.yaml", "python_model.pkl", "requirements.txt"]
    encoded_data = {}

    for file in files:
        with open(os.path.join(model_path, file), 'rb') as f:
            encoded_data[file] = base64.b64encode(f.read()).decode('utf-8')

    if os.path.exists(model_path):
        shutil.rmtree(model_path)

    data = {"model_name": model_name, **encoded_data}

    url = f'{MODEL_SERVICE_ENDPOINT}/log_model'

    try:
        response = requests.post(url, json=data, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as err:
        return {'state': False, 'message': str(err)}


def _SEND_RESULT(model_id: str, params, metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Send the model result to the server.

    Args:
        model_id (str): Model ID.
        params: Parameters including run_id, execution_date, and dag_id.
        metrics (Dict[str, Any]): Dictionary of model metrics.

    Returns:
        Dict[str, Any]: Server response.
    """
    url = f'{ALERT_SERVER_ENDPOINT}/api/v1/dags/response/put'
    payload = {
        'dag_id': params.dag_id,
        'execution_date': params.execution_date.isoformat(),
        'run_id': params.run_id,
        'model_id': model_id,
        'metrics': metrics,
    }

    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as err:
        return {'state': False, 'message': str(err)}


def _SEND_PREDICTION(model_id: str, params, prediction: pd.DataFrame) -> Dict[str, Any]:
    """Send model predictions to the server.

    Args:
        model_id (str): Model ID.
        params: Parameters including run_id, execution_date, and dag_id.
        prediction (pd.DataFrame): Dataframe containing predictions.

    Returns:
        Dict[str, Any]: Server response.
    """
    url = f'{ALERT_SERVER_ENDPOINT}/api/v1/dags/response/put'
    payload = {
        'execution_date': params.execution_date.isoformat(),
        'dag_id': params.dag_id,
        'model_id': model_id,
        'run_id': params.run_id,
        'result_path': f'/predictions/{model_id}/{params.run_id}',
        'prediction': prediction.to_json(orient='records'),
    }

    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as err:
        return {'state': False, 'message': str(err)}


def _SAVE_PREDICTION(model_id: str, run_id: str, prediction: pd.DataFrame):
    """Save prediction to HDFS.

    Args:
        model_id (str): Model ID.
        run_id (str): Run ID.
        prediction (pd.DataFrame): Prediction dataframe.

    Returns:
        None
    """
    translation_table = str.maketrans(string.punctuation, '-' * len(string.punctuation))
    run_id_str = run_id.translate(translation_table)
    path = f'/predictions/{model_id}/{run_id_str}'

    client = InsecureClient("http://hadoop-platform:9870", user="hdoop")
    client.makedirs(path)

    with client.write(os.path.join(path, "prediction.csv"), overwrite=True) as writer:
        prediction.to_csv(writer, index=True)


def _GET_PREDICTION(path: str, target_name: str) -> Tuple[pd.Series, pd.Series]:
    """Retrieve prediction from HDFS.

    Args:
        path (str): Path to the prediction file.
        target_name (str): Target column name.

    Returns:
        Tuple[pd.Series, pd.Series]: Tuple containing the target values and mapping UIDs.
    """
    client = InsecureClient("http://hadoop-platform:9870", user="hdoop")
    with client.read(f"{path}/prediction.csv") as reader:
        prediction = pd.read_csv(reader)

    return prediction[target_name], prediction["mapping_uid"]
