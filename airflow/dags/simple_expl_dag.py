from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

import mlflow
from mlflow.models import infer_signature
import pandas as pd, matplotlib.pyplot as plt, seaborn as sns, json, os

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.datasets import load_iris

# Define constants
INPUT_FILE_EXPL = os.environ.get("INPUT_FILE_EXPL")
MLFLOW_URI = os.environ.get("MLFLOW_URI")
MLFLOW_EXPERIMENT_EXPLAINABILITY_NAME = os.environ.get("MLFLOW_EXPERIMENT_EXPLAINABILITY_NAME")
MLFLOW_EXPERIMENT_ML_EXPL_NAME =  os.environ.get("MLFLOW_EXPERIMENT_ML_EXPL_NAME")
MONGO_CONN_ID = os.environ.get("MONGO_CONN_ID")
MONGO_ETL_LOAD_COLLECTION = os.environ.get("MONGO_ETL_LOAD_COLLECTION")

MONGO_EXPL_DATABASE = os.environ.get("MONGO_EXPL_DATABASE")
MONGO_EXPL_EXTRACT_COLLECTION = os.environ.get("MONGO_EXPL_EXTRACT_COLLECTION")
MONGO_EXPL_TRANSFORM_COLLECTION = os.environ.get("MONGO_EXPL_TRANSFORM_COLLECTION")
MONGO_EXPL_TRAIN_COLLECTION = os.environ.get("MONGO_EXPL_TRAIN_COLLECTION")
MONGO_EXPL_EXPLAIN_COLLECTION = os.environ.get("MONGO_EXPL_EXPLAIN_COLLECTION")

# Task 1: Extract data
def extract():
    # Read data from file
    df = pd.read_csv(INPUT_FILE_EXPL)
    data = df.to_dict(orient='records')
    
    # Mongo connection
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db = client[MONGO_EXPL_DATABASE]

    # Store extracted data in a collection
    ml_collection = db[MONGO_EXPL_EXTRACT_COLLECTION]
    ml_collection.delete_many({})
    ml_collection.insert_many(data)

# Task 2: Preprocess data
def preprocessing():

    # Mongo connection
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db = client[MONGO_EXPL_DATABASE]

    # Retrieve data from the previous Step's collection
    load_collection = db[MONGO_EXPL_EXTRACT_COLLECTION]
    data = list(load_collection.find({}))
    df = pd.DataFrame(data)
    
    # Perform data transformation
    transformed_data = df.to_dict(orient='records')

    # Store tramsformed data in a collection    
    preprocessed_collection = db[MONGO_EXPL_TRANSFORM_COLLECTION]
    preprocessed_collection.delete_many({})
    preprocessed_collection.insert_many(transformed_data)


# Task 3: Train a machine learning model
def train_model():
    
    # Mongo connection
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db = client[MONGO_EXPL_DATABASE]
    
    # Retrieve data from the previous Step's collection
    load_collection = db[MONGO_EXPL_TRANSFORM_COLLECTION]
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_EXPLAINABILITY_NAME)

    # Load the dataset and convert to a DataFrame
    data = pd.DataFrame(list(load_collection.find({})),
                        columns=[
                            "SepalLengthCm", "SepalWidthCm", "PetalLengthCm",
                            "PetalWidthCm", "Species"
                        ])

    # Store tramsformed data in a collection 
    trained_collection = db[MONGO_EXPL_TRAIN_COLLECTION]
    trained_collection.delete_many({})
    trained_collection.insert_many(data.to_dict(orient='records'))

# Task 4: Explain machine learning model
def explain_model():
    
    # Mongo connection
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db = client[MONGO_EXPL_DATABASE]

    # Retrieve data from the previous Step's collection
    load_collection = db[MONGO_EXPL_TRAIN_COLLECTION]
    data = pd.DataFrame(list(load_collection.find({})),
                        columns=[
                            "SepalLengthCm", "SepalWidthCm", "PetalLengthCm",
                            "PetalWidthCm", "Species"
                        ])
        
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_ML_EXPL_NAME)    
    
# Define default arguments for DAG
default_args = {'owner': os.environ.get("MONGO_INITDB_ROOT_USERNAME")}

# Define the DAG
dag = DAG(
    'simple_expl_pipeline',
    default_args=default_args,
    description='A simple ML & Explainability pipeline with four tasks',
    schedule_interval=None,
)

# Define the tasks using PythonOperator
extract_task = PythonOperator(
    task_id=MONGO_EXPL_EXTRACT_COLLECTION,
    python_callable=extract,
    dag=dag,
)

preprocessing_task = PythonOperator(
    task_id=MONGO_EXPL_TRANSFORM_COLLECTION,
    python_callable=preprocessing,
    dag=dag,
)

train_model_task = PythonOperator(
    task_id=MONGO_EXPL_TRAIN_COLLECTION,
    python_callable=train_model,
    dag=dag,
)

explain_model_task = PythonOperator(
    task_id=MONGO_EXPL_EXPLAIN_COLLECTION,
    python_callable=train_model,
    dag=dag,
)

extract_task >> preprocessing_task >> train_model_task >> explain_model_task
