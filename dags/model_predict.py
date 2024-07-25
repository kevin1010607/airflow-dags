import os
import numpy as np
import pandas as pd
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from util import _DC, _EXTRACT_DATETIME, _FE, _GET_MOTION, _LOAD_MODEL, _ONEHOT_ENCODING, _SAVE_PREDICTION, _SEND_PREDICTION, _SEND_RESULT


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
def model_predict():
    """DAG for model prediction."""

    @task
    def data_collect():
        """Collect data for prediction.
        
        Returns:
            dict: A dictionary containing raw data, features name, and targets name.
        """
        context = get_current_context()
        params = context['params']
        date = params['date']
        lot_id = params['lot_id']

        raw_data = _GET_MOTION(date, lot_id)

        return raw_data
    
    @task
    def data_preprocess(raw_data):
        """Preprocess the collected data.
        
        Args:
            raw_data (pd.DataFrame): The raw data.
            features_name (list): List of feature column names.
        
        Returns:
            dict: A dictionary containing test features.
        """
        context = get_current_context()
        params = context['params']
        features_name = params['features_name']
        features_name.append('mapping_uid')
        data = raw_data[features_name]

        # Preprocess the remaining columns
        feature = _DC(data)
        uid_column = feature['mapping_uid']
        feature = feature.drop('mapping_uid', axis=1)
        feature = _EXTRACT_DATETIME(feature)
        feature = _ONEHOT_ENCODING(feature)

        # Add 'uid' column to the preprocessed features DataFrame
        feature['mapping_uid'] = uid_column

        return feature
    
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
    def predict(model, test_feature):
        """Make predictions using the loaded model.
        
        Args:
            model (Any): The loaded model.
            test_feature (pd.DataFrame): The test features.
            targets_name (list): List of target column names.
        
        Returns:
            pd.DataFrame: The predictions.
        """
        ctx = get_current_context()
        params = ctx['params']
        targets_name = params['targets_name']
        test_feature_without_uid = test_feature.drop('mapping_uid', axis=1)

        # Step 2: Make predictions using the preprocessed features
        feature_tensor = test_feature_without_uid.values
        prediction_tensor = model.predict(feature_tensor)

        # Step 3: Create the prediction DataFrame with the 'uid' column
        prediction = pd.DataFrame(prediction_tensor, columns=targets_name)
        prediction['mapping_uid'] = test_feature['mapping_uid']
        
        return prediction
    
    @task
    def save(prediction):
        """Save the predictions.
        
        Args:
            prediction (pd.DataFrame): The predictions to save.
        
        Returns:
            None
        """
        context = get_current_context()
        params = context['params']
        model_id = params['model_id']
        params = context['dag_run']
        run_id = params.run_id

        try:
            _SAVE_PREDICTION(model_id, run_id, prediction)
        except Exception as e:
            print(f"Error occurred while saving prediction data: {e}")

    
    @task
    def send_prediction(prediction):
        context = get_current_context()
        params = context['params']
        model_id = params['model_id']
        params = context['dag_run']

        response = _SEND_PREDICTION(model_id, params, prediction)
        return response
    
    # Task: data collect
    raw_data = data_collect()
    # Task: data preprocess
    test_feature = data_preprocess(raw_data)
    # Task: load model
    model = load_model()
    # Task: predict
    prediction = predict(model, test_feature)
    # Task: save
    save_response = save(prediction)
    # Task: send prediction
    send_prediction_response = send_prediction(prediction)

# Define the DAG
predict_dag = model_predict()
