from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
from datetime import datetime

# Define constants
INPUT_FILE = '/opt/airflow/dags/utils/iris.csv'
MONGO_CONN_ID = 'mongo_default'
MONGO_ETL_DATABASE = 'seminar'
MONGO_EXTRACT_COLLECTION = 'etl_extract'
MONGO_TRANSFORM_COLLECTION = 'etl_transform'
MONGO_FINAL_COLLECTION = 'etl_load'

def extract():
    """Extract data from CSV and store in MongoDB."""
    df = pd.read_csv(INPUT_FILE)
    data = df.to_dict(orient='records')
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db = client[MONGO_ETL_DATABASE]
    
    # Store extracted data
    collection = db[MONGO_EXTRACT_COLLECTION]
    collection.insert_many(data)
    
    print(f"Extracted {len(data)} records into MongoDB.")

def transform():
    """Retrieve extracted data from MongoDB, transform it, and store back."""
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db = client[MONGO_ETL_DATABASE]

    extract_collection = db[MONGO_EXTRACT_COLLECTION]
    transform_collection = db[MONGO_TRANSFORM_COLLECTION]

    data = list(extract_collection.find({}))  # Retrieve extracted data

    df = pd.DataFrame(data)
    # df.columns = [col.upper() for col in df.columns]  # Normalize column names
    df = df.drop('Id', axis=1)  # Example cleaning

    transformed_data = df.to_dict(orient='records')
    transform_collection.insert_many(transformed_data)

    print(f"Transformed {len(transformed_data)} records and stored them in MongoDB.")

def load():
    """Retrieve transformed data from MongoDB and insert into final collection."""
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db = client[MONGO_ETL_DATABASE]

    transform_collection = db[MONGO_TRANSFORM_COLLECTION]
    final_collection = db[MONGO_FINAL_COLLECTION]

    data = list(transform_collection.find({}))  # Retrieve transformed data

    final_collection.insert_many(data)  # Load data into final MongoDB collection
    print(f"Loaded {len(data)} records into final MongoDB collection '{MONGO_FINAL_COLLECTION}'.")

# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
}

with DAG(
    'etl_mongo_dag_inter',
    default_args=default_args,
    description='ETL pipeline using MongoDB as intermediate storage',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load,
    )

    extract_task >> transform_task >> load_task
