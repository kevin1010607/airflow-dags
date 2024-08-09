import os
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from util import (
    _DC, _EXTRACT_DATETIME, _FE, _GET_MOTION_AND_QA, _LOAD_MODEL, 
    _LOG_MODEL, _LOG_METRIC, _LOG_PARAMETER, _ONEHOT_ENCODING, _SEND_RESULT, generate_mock_data, 
)


@dag(
    schedule=None, 
    start_date=datetime(2024, 1, 1), 
    tags=[], 
    params={
        'model_id': 'multi_y_regressor', 
        'model_version': 'latest',
        'date': "2024-01-01", 
        'lot_id': "ATWLOT-010124-0852-553-001", 
        'features_name': ['equipment_id','lf_id','proc_datetime',  'heat_pre'], 
        'targets_name': [
            "al_squeeze_out_x",
            "al_squeeze_out_y",
            "outer_ball_size_x",
            "outer_ball_size_y",
            "ball_thickness",
            "loop_height",
            "outer_ball_shape",
            "inner_ball_shape",
        ]
    }, 
)
def model_update():
    """DAG for updating, evaluating, and logging model metrics."""

    @task
    def data_collect():
        """Collect data for model training.
        
        Returns:
            dict: A dictionary containing raw data, features name, and targets name.
        """
        context = get_current_context()
        params = context['params']
        date = params['date']
        lot_id = params['lot_id']

        # raw_data = _GET_MOTION_AND_QA(date, lot_id)
        raw_data = generate_mock_data()
    
        return raw_data
    
    @task(multiple_outputs=True)
    def data_preprocess(raw_data):
        """Preprocess the collected data.
        
        Args:
            raw_data (pd.DataFrame): The raw data.
            features_name (list): List of feature column names.
            targets_name (list): List of target column names.
        
        Returns:
            dict: A dictionary containing train/test features and targets.
        """
        context = get_current_context()
        params = context['params']
        features_name = params['features_name']
        targets_name = params['targets_name']
        data = raw_data[features_name+targets_name]
        data = _DC(data)
        feature, target = _FE(data, features_name, targets_name)
        feature  = _EXTRACT_DATETIME(feature)
        feature = _ONEHOT_ENCODING(feature)
        
        train_feature, test_feature, train_target, test_target = train_test_split(feature, target, test_size=0.2, random_state=42)
        
        return {
            'train_feature': train_feature, 
            'test_feature': test_feature, 
            'train_target': train_target, 
            'test_target': test_target, 
        }
    
    @task
    def load_model():
        """Load the model from the server.
        
        Returns:
            Any: The loaded model.
        """
        context = get_current_context()
        params = context['params']
        model_id = params['model_id']
        model_version = params['model_version']

        model = _LOAD_MODEL(model_id, model_version)

        return model
    
    @task
    def update(model, train_feature, train_target):
        """Update the model with the training data.
        
        Args:
            model (Any): The loaded model.
            train_feature (pd.DataFrame): The training features.
            train_target (pd.DataFrame): The training targets.
        
        Returns:
            Any: The updated model.
        """
        feature_tensor = train_feature.values
        target_tensor = train_target.values

        model.fit(feature_tensor, target_tensor)

        return model
    
    @task
    def save(model):
        """Save the updated model to the server.
        
        Args:
            model (Any): The updated model.
        
        Returns:
            dict: The response from the server.
        """
        context = get_current_context()
        params = context['params']
        model_id = params['model_id']

        response = _LOG_MODEL(model_id, model)

        return response

    @task
    def predict(model, test_feature):
        """Make predictions using the updated model.
        
        Args:
            model (Any): The updated model.
            test_feature (pd.DataFrame): The test features.
            targets_name (list): List of target column names.
        
        Returns:
            pd.DataFrame: The predictions.
        """
        ctx = get_current_context()
        params = ctx['params']
        targets_name = params['targets_name']
        feature_tensor = test_feature.values

        prediction_tensor = model.predict(feature_tensor)
        prediction = pd.DataFrame(prediction_tensor, columns=targets_name)
        
        return prediction
    
    @task
    def evaluate(target, prediction):
        """Evaluate the model predictions.
        
        Args:
            target (pd.DataFrame): The true target values.
            prediction (pd.DataFrame): The predicted values.
        
        Returns:
            dict: A dictionary containing evaluation metrics.
        """
        _r2_score = r2_score(target, prediction)
        _mse_score = mean_squared_error(target, prediction)
        errors = prediction.values - target.values
        _pos_max_err = errors.max()
        _neg_max_err = errors.min()

        return {
            'r2_score': _r2_score, 
            'mse_score': _mse_score, 
            'pos_max_err': _pos_max_err, 
            'neg_max_err': _neg_max_err, 
        }
    
    @task
    def collect_metric(metrics):
        """Log evaluation metrics to the server.
        
        Args:
            metrics (dict): The evaluation metrics.
        
        Returns:
            dict: The responses from the server.
        """
        context = get_current_context()
        params = context['params']
        model_id = params['model_id']
        
        responses = {
            'metric': _LOG_METRIC(model_id, metrics), 
            'parameter': _LOG_PARAMETER(model_id, {})
        }

        return responses
    
    @task
    def send_result(metrics):
        context = get_current_context()
        params = context['params']
        model_id = params['model_id']
        dag_params = context['dag_run']

        response = _SEND_RESULT(model_id, dag_params, metrics)
        return response
    
    # Task: data collect
    raw_data = data_collect()
    # Task: data preprocess
    data_preprocess_result = data_preprocess(raw_data)
    train_feature = data_preprocess_result['train_feature']
    test_feature = data_preprocess_result['test_feature']
    train_target = data_preprocess_result['train_target']
    test_target = data_preprocess_result['test_target']
    # Task: load model
    model = load_model()
    # Task: update
    model = update(model, train_feature, train_target)
    # Task: save
    save_response = save(model)
    # Task: predict
    prediction = predict(model, test_feature)
    # Task: evaluate
    metrics = evaluate(test_target, prediction)
    # Task: collect metric
    collect_metric_response = collect_metric(metrics)
    # Task: send result
    send_result_response = send_result(metrics)


# Define the DAG
update_dag = model_update()
