import base64
import os
import shutil
import string
import mlflow
import numpy as np
import pandas as pd
from typing import Tuple, List, Dict, Any, Union
import requests
from catboost import CatBoostClassifier, CatBoostRegressor
from sklearn.multioutput import MultiOutputClassifier, MultiOutputRegressor
from pyspark.sql import DataFrame, SparkSession
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEBUG = True
ALERT_SERVER_ENDPOINT = 'http://10.121.252.194:5111'
RESULT_SERVER_ENDPOINT = 'http://10.121.252.194:5888'
ML_SERVICE_ENDPOINT = 'model-service-service:7777'


def _DC(data: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with missing values from the dataframe.
    
    Args:
        data (pd.DataFrame): The input dataframe.
    
    Returns:
        pd.DataFrame: The dataframe with missing values dropped.
    """
    return data.dropna()


def _FE(data: pd.DataFrame, features_name: List[str] = [], targets_name: List[str] = []) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Extract features and target variables from the dataframe.
    
    Args:
        data (pd.DataFrame): The input dataframe.
        features_name (List[str]): List of feature column names.
        targets_name (List[str]): List of target column names.
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the features and targets dataframes.
    
    Raises:
        Exception: If features or targets contain non-numeric values.
    """
    feature = data[features_name]
    target = data[targets_name]
    
    return feature, target


def determine_data_type(y):
    # Determine the type of the target variable (y)
    if y.shape[1] > 1:
        if np.issubdtype(y.dtype, np.integer):
            return "multi_y_classification"
        else:
            return "multi_y_regression"
    else:
        if np.issubdtype(y.dtype, np.integer):
            return "single_y_classification"
        else:
            return "single_y_regression"

def _GET_MODEL(y):
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


def _LOAD_MODEL(model_name, model_version):

    url = f'{ML_SERVICE_ENDPOINT}/load_model'
    data = {"model_name": model_name, "model_version": model_version}
    response = requests.post(url, json=data)
    result = response.json()["result"]

    conda = result["conda"]
    MLmodel = result["MLmodel"]
    python_env = result["python_env"]
    python_model = result["python_model"]
    requirements = result["requirements"]

    # remove tmp file
    model_path = "model"
    if os.path.exists(model_path):
        shutil.rmtree(model_path)

    # create a dir to place all the stuff
    os.mkdir(model_path)

    # decode all the stuff
    decoded_conda = base64.b64decode(conda)
    decoded_MLmodel = base64.b64decode(MLmodel)
    decoded_python_env = base64.b64decode(python_env)
    decoded_python_model = base64.b64decode(python_model)
    decoded_requirements = base64.b64decode(requirements)

    # write all the stuff to disk
    with open(os.path.join(model_path, "conda.yaml"), 'wb') as file:
        file.write(decoded_conda)
    with open(os.path.join(model_path, "MLmodel"), 'wb') as file:
        file.write(decoded_MLmodel)
    with open(os.path.join(model_path, "python_env.yaml"), 'wb') as file:
        file.write(decoded_python_env)
    with open(os.path.join(model_path, "python_model.pkl"), 'wb') as file:
        file.write(decoded_python_model)
    with open(os.path.join(model_path, "requirements.txt"), 'wb') as file:
        file.write(decoded_requirements)

    # load model from file
    loaded_model = mlflow.pyfunc.load_model(model_path)
    unwrapped_model = loaded_model.unwrap_python_model()

    if os.path.exists(model_path):
        shutil.rmtree(model_path)

    return unwrapped_model.model

def _LOG_METRIC(model_id: str, metrics) -> Dict[str, Any]:
    """Log a performance metric for a model to the server.
    
    Args:
        model_id (str): The ID of the model.
        metric (str): The key of the metric.
    
    Returns:
        Dict[str, Any]: The response from the server.
    
    Raises:
        Exception: If logging the metric fails.
    """
    url = f'{ML_SERVICE_ENDPOINT}/log_metrics'
    payload = {
        'model_name': model_id, 
        'metrics': metrics,
    }
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 500:
            raise Exception(f'status_code: {response.status_code}, error: {response.json()}')
        
        response.raise_for_status() # Raises HTTPError for bad responses (4xx and 5xx)
        raise Exception(f'Unexpected status code received: {response.status_code}')

    except Exception as err:
        return {'state': False, 'message': str(err)}


def _LOG_PARAMETER(model_id: str, parameter) -> Dict[str, Any]:
    """Log a parameter for a model to the server.
    
    Args:
        model_id (str): The ID of the model.
        param_key (str): The key of the parameter.
        param_val (float): The value of the parameter.
    
    Returns:
        Dict[str, Any]: The response from the server.
    
    Raises:
        Exception: If logging the parameter fails.
    """
    parameter['model_status'] = 'normal'
    url = f'{ML_SERVICE_ENDPOINT}/log_parameters'
    payload = {
        'model_name': model_id, 
        'parameters': parameter,
    }
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 500:
            raise Exception(f'status_code: {response.status_code}, error: {response.json()}')
        
        response.raise_for_status() # Raises HTTPError for bad responses (4xx and 5xx)
        raise Exception(f'Unexpected status code received: {response.status_code}')

    except Exception as err:
        return {'state': False, 'message': str(err)}


def _GET_MOTION_AND_QA(date:str, lot_id:str )-> pd.DataFrame:

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
    sql_str = \
    f"SELECT * FROM ( SELECT * FROM ( SELECT mapping_uid, measure_item, value FROM qa WHERE (date = '{date}' AND lot_id = '{lot_id}') ) t PIVOT ( FIRST(value) FOR measure_item IN ('ball_shear', 'neck_pull', 'wire_pull', 'stitch_pull', 'outer_ball_size_x', 'outer_ball_size_y', 'inner_ball_size_x', 'inner_ball_size_y', 'outer_ball_shape', 'inner_ball_shape', 'ball_placement_x', 'ball_placement_y', 'al_squeeze_out_x', 'al_squeeze_out_y', 'ball_thickness', 'loop_height', 'anomaly_bb', 'anomaly_bs') ) ) t1 INNER JOIN ( SELECT * FROM motion_bb_mfdotwbdot000001 WHERE (date = '{date}' AND lot_id = '{lot_id}') ) t2 USING(mapping_uid)"

    df = client.sql(sql_str)
    pdf = df.toPandas()
    client.stop()

    return pdf

def _GET_MOTION(date:str, lot_id:str )-> pd.DataFrame:

    client = (
        SparkSession.builder.appName("data_analyzer")
        .config("spark.hive.metastore.uris", "thrift://hadoop-platform:9083")
        .enableHiveSupport()
        .master("spark://hadoop-platform:7077")
        .getOrCreate()
    )
    motion_sql_str = \
    f"SELECT * FROM motion_bb_mfdotwbdot000001 WHERE (date = '{date}' AND lot_id = '{lot_id}') LIMIT 100"

    df = client.sql(motion_sql_str)
    pdf = df.toPandas()
    client.stop()

    return pdf

def _GET_QA(date:str, lot_id:str )-> pd.DataFrame:

    client = (
        SparkSession.builder.appName("data_analyzer")
        .config("spark.hive.metastore.uris", "thrift://hadoop-platform:9083")
        .enableHiveSupport()
        .master("spark://hadoop-platform:7077")
        .getOrCreate()
    )
    qa_sql_str = \
    f"SELECT * FROM ( SELECT * FROM ( SELECT mapping_uid, measure_item, value FROM qa WHERE (date = '{date}' AND lot_id = '{lot_id}') ) t PIVOT ( FIRST(value) FOR measure_item IN ('ball_shear', 'neck_pull', 'wire_pull', 'stitch_pull', 'outer_ball_size_x', 'outer_ball_size_y', 'inner_ball_size_x', 'inner_ball_size_y', 'outer_ball_shape', 'inner_ball_shape', 'ball_placement_x', 'ball_placement_y', 'al_squeeze_out_x', 'al_squeeze_out_y', 'ball_thickness', 'loop_height', 'anomaly_bb', 'anomaly_bs') ) ) t1 INNER JOIN ( SELECT mapping_uid FROM motion_bb_mfdotwbdot000001 WHERE (date = '{date}' AND lot_id = '{lot_id}') ) t2 USING(mapping_uid)"

    df = client.sql(qa_sql_str)
    pdf = df.toPandas()
    client.stop()

    return pdf
def _ONEHOT_ENCODING(df:pd.DataFrame) -> pd.DataFrame:

    non_numeric_columns = df.select_dtypes(exclude=['float64', 'int64']).columns
    if(len(non_numeric_columns) == 0):
        return df
    else :
        one_hot_encoded = pd.get_dummies(df[non_numeric_columns])
        df_encoded = pd.concat([df, one_hot_encoded], axis=1)
        df_encoded = df_encoded.drop(non_numeric_columns, axis=1)
        return df_encoded

def _EXTRACT_DATETIME(df: pd.DataFrame) -> pd.DataFrame:
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

def _LOG_MODEL(model_name, model):
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

    with open(os.path.join(model_path, "conda.yaml"), 'rb') as file:
        conda = base64.b64encode(file.read()).decode('utf-8')
    with open(os.path.join(model_path, "MLmodel"), 'rb') as file:
        MLmodel = base64.b64encode(file.read()).decode('utf-8')
    with open(os.path.join(model_path, "python_env.yaml"), 'rb') as file:
        python_env = base64.b64encode(file.read()).decode('utf-8')
    with open(os.path.join(model_path, "python_model.pkl"), 'rb') as file:
        python_model = base64.b64encode(file.read()).decode('utf-8')
    with open(os.path.join(model_path, "requirements.txt"), 'rb') as file:
        requirements = base64.b64encode(file.read()).decode('utf-8')

    data = {
        "model_name": model_name,
        "conda": conda,
        "MLmodel": MLmodel,
        "python_env": python_env,
        "python_model": python_model,
        "requirements": requirements
    }

    if os.path.exists(model_path):
        shutil.rmtree(model_path)

    url = f'{ML_SERVICE_ENDPOINT}/log_model'

    try:
        response = requests.post(url, json=data, timeout=10)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 500:
            raise Exception(f'status_code: {response.status_code}, error: {response.json()}')
        
        response.raise_for_status() # Raises HTTPError for bad responses (4xx and 5xx)
        raise Exception(f'Unexpected status code received: {response.status_code}')

    except Exception as err:
        return {'state': False, 'message': str(err)}

def _SEND_RESULT(model_id, params, metrics):
    url = f'{RESULT_SERVER_ENDPOINT}/api/v1/dags/response/put'
    run_id = params.run_id
    execution_date = params.execution_date
    dag_id = params.dag_id

    payload = { 
        'dag_id': dag_id,
        'execution_date': execution_date.isoformat(),
        'run_id': run_id, 
        'model_id': model_id,
        'metrics': metrics, 
    }
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 500:
            raise Exception(f'status_code: {response.status_code}, error: {response.json()}')
        
        response.raise_for_status() # Raises HTTPError for bad responses (4xx and 5xx)
        raise Exception(f'Unexpected status code received: {response.status_code}')

    except Exception as err:
        return {'state': False, 'message': str(err)}


def _SEND_PREDICTION(model_id, params, prediction):
    run_id = params.run_id
    execution_date = params.execution_date
    dag_id = params.dag_id

    url = f'{RESULT_SERVER_ENDPOINT}/api/v1/dags/response/put'
    payload = {
        'execution_date': execution_date.isoformat(),
        'dag_id': dag_id,
        'model_id': model_id, 
        'run_id': run_id, 
        'result_path': f'/predictions/{model_id}/{run_id}',
        'prediction': prediction.to_json(orient='records'), 
    }
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 500:
            raise Exception(f'status_code: {response.status_code}, error: {response.json()}')
        
        response.raise_for_status() # Raises HTTPError for bad responses (4xx and 5xx)
        raise Exception(f'Unexpected status code received: {response.status_code}')

    except Exception as err:
        return {'state': False, 'message': str(err)}


def _SAVE_PREDICTION(model_id, run_id, prediction):

    # Create a translation table that maps each punctuation character to a hyphen
    translation_table = str.maketrans(string.punctuation, '-' * len(string.punctuation))
    
    # Use the translation table to replace punctuation with hyphens
    run_id_str = run_id.translate(translation_table)
    path = f'/predictions/{model_id}/{run_id_str}'
    # make hdfs connection
    client = InsecureClient("http://hadoop-platform:9870", user="hdoop")
    # create the directory
    client.makedirs(path)

    # save the data
    with client.write(
        os.path.join(path, "prediction.csv"), overwrite=True
    ) as writer:
        prediction.to_csv(writer, index=True)

def _GET_PREDICTION(path: str, target_name):
    path = path+"/prediction.csv"
    # make hdfs connection
    client  = InsecureClient("http://hadoop-platform:9870", user="hdoop")
    # read the data
    with client.read(path) as reader:
        prediction = pd.read_csv(reader)

    return prediction[target_name], prediction["mapping_uid"]