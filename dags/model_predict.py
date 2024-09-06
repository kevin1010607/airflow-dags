import os
import numpy as np
import pandas as pd
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from util import (
    _DC, _EXTRACT_DATETIME, _FE, _GET_MOTION, _LOAD_MODEL,
    _ONEHOT_ENCODING, _SAVE_PREDICTION, _SEND_PREDICTION, _SEND_RESULT
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
        'features_name': ['equipment_id', 'lf_id', 'proc_datetime', 'heat_pre'],
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
            pd.DataFrame: Raw data for prediction.
        """
        try:
            context = get_current_context()
            params = context['params']
            date = params['date']
            lot_id = params['lot_id']

            print(f"Collecting motion data from date: {date}, lot_id: {lot_id}")
            raw_data = _GET_MOTION(date, lot_id)

            return raw_data

        except Exception as e:
            print(f"Error in data_collect: {str(e)}")
            raise e

    @task
    def data_preprocess(raw_data):
        """Preprocess the collected data.

        Args:
            raw_data (pd.DataFrame): The raw data.

        Returns:
            pd.DataFrame: Preprocessed features including mapping_uid.
        """
        try:
            context = get_current_context()
            params = context['params']
            features_name = params['features_name']
            features_name.append('mapping_uid')
            data = raw_data[features_name]

            print("Preprocessing data")
            feature = _DC(data)
            uid_column = feature['mapping_uid']
            feature = feature.drop('mapping_uid', axis=1)

            print("Extracting datetime and one-hot encoding")
            feature = _EXTRACT_DATETIME(feature)
            feature = _ONEHOT_ENCODING(feature)

            # Add 'uid' column to the preprocessed features DataFrame
            feature['mapping_uid'] = uid_column

            return feature

        except Exception as e:
            print(f"Error in data_preprocess: {str(e)}")
            raise e

    @task
    def load_model():
        """Load the model from the server.

        Returns:
            Any: The loaded model.
        """
        try:
            context = get_current_context()
            params = context['params']
            model_id = params['model_id']
            model_version = params['model_version']

            print(f"Loading model: {model_id}, version: {model_version}")
            model = _LOAD_MODEL(model_id, model_version)

            return model

        except Exception as e:
            print(f"Error in load_model: {str(e)}")
            raise e

    @task
    def predict(model, test_feature):
        """Make predictions using the loaded model.

        Args:
            model (Any): The loaded model.
            test_feature (pd.DataFrame): The test features.

        Returns:
            pd.DataFrame: The predictions.
        """
        try:
            ctx = get_current_context()
            params = ctx['params']
            targets_name = params['targets_name']
            test_feature_without_uid = test_feature.drop('mapping_uid', axis=1)

            print("Making predictions")
            feature_tensor = test_feature_without_uid.values
            prediction_tensor = model.predict(feature_tensor)

            print("Creating prediction DataFrame with mapping_uid")
            prediction = pd.DataFrame(prediction_tensor, columns=targets_name)
            prediction['mapping_uid'] = test_feature['mapping_uid']

            return prediction

        except Exception as e:
            print(f"Error in predict: {str(e)}")
            raise e

    @task
    def save(prediction):
        """Save the predictions.

        Args:
            prediction (pd.DataFrame): The predictions to save.

        Returns:
            None
        """
        try:
            context = get_current_context()
            params = context['params']
            model_id = params['model_id']
            run_id = context['dag_run'].run_id

            print(f"Saving predictions for model: {model_id}, run_id: {run_id}")
            _SAVE_PREDICTION(model_id, run_id, prediction)

        except Exception as e:
            print(f"Error in save: {str(e)}")
            raise e

    @task
    def send_prediction(prediction):
        """Send the predictions to the result server.

        Args:
            prediction (pd.DataFrame): The predictions to send.

        Returns:
            dict: The response from the server.
        """
        try:
            context = get_current_context()
            params = context['params']
            model_id = params['model_id']
            dag_params = context['dag_run']

            print("Sending predictions to result server")
            response = _SEND_PREDICTION(model_id, dag_params, prediction)
            return response

        except Exception as e:
            print(f"Error in send_prediction: {str(e)}")
            raise e

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