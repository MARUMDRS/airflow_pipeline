from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
import pandas as pd, os, pendulum, effector

local_tz = pendulum.timezone("Europe/Athens")
# Define constants
INPUT_FILE = os.environ.get("INPUT_FILE") # The path to the input file defined through environment variable (.env)
MONGO_CONN_ID = os.environ.get("MONGO_CONN_ID") # Mongo connection id defined through environment variable (.env)
MONGO_ETL_DATABASE = os.environ.get("MONGO_ETL_DATABASE") # Mongo Database defined through environment variable (.env)
MONGO_ETL_EXTRACT_COLLECTION = os.environ.get("MONGO_ETL_EXTRACT_COLLECTION") # Mongo Collection for "extract" step defined through environment variable (.env)
MONGO_ETL_TRANSFORM_COLLECTION = os.environ.get(
    "MONGO_ETL_TRANSFORM_COLLECTION") # Mongo Collection for "tramsform" step defined through environment variable (.env)
MONGO_ETL_LOAD_COLLECTION = os.environ.get("MONGO_ETL_LOAD_COLLECTION") # Mongo Collection for "load" step defined through environment variable (.env)


def extract(): 
    """Extract data from CSV and store in MongoDB."""
    # Read data from file
    df = pd.read_csv(INPUT_FILE)
    data = df.to_dict(orient='records')
    
    # Mongo connection
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db = client[MONGO_ETL_DATABASE]

    # Store extracted data in a collection
    collection = db[MONGO_ETL_EXTRACT_COLLECTION]
    collection.delete_many({})
    collection.insert_many(data)

    print(f"Extracted {len(data)} records into MongoDB.")


def transform():
    """Retrieve extracted data from MongoDB, transform it, and store back."""
    # Mongo connection
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db = client[MONGO_ETL_DATABASE]

    # Retrieve data from the previous Step's collection
    extract_collection = db[MONGO_ETL_EXTRACT_COLLECTION]
    data = list(extract_collection.find({}))  # Retrieve extracted data
    df = pd.DataFrame(data)
    
    # Perform data transformation
    df = df.drop('Id', axis=1)
    transformed_data = df.to_dict(orient='records')

    # Store tramsformed data in a collection
    transform_collection = db[MONGO_ETL_TRANSFORM_COLLECTION]
    transform_collection.delete_many({})
    transform_collection.insert_many(transformed_data)

    print(
        f"Transformed {len(transformed_data)} records and stored them in MongoDB."
    )


def load():
    """Retrieve transformed data from MongoDB and insert into final collection."""
    # Mongo connection
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db = client[MONGO_ETL_DATABASE]

    # Retrieve transformed data from the previous Step's collection
    transform_collection = db[MONGO_ETL_TRANSFORM_COLLECTION]
    data = list(transform_collection.find({}))  # Retrieve transformed data
    
    # Store transformed data in the final collection
    final_collection = db[MONGO_ETL_LOAD_COLLECTION]
    final_collection.delete_many({})
    final_collection.insert_many(data)
    print(
        f"Loaded {len(data)} records into final MongoDB collection '{MONGO_ETL_LOAD_COLLECTION}'."
    )


# Define default arguments for DAG
default_args = {
    'owner': os.environ.get("MONGO_INITDB_ROOT_USERNAME"),
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 1, tzinfo=local_tz),
    'retries': 1,
}

# Define the DAG
dag = DAG('simple_etl_pipeline',
          default_args=default_args,
          description='ETL pipeline using MongoDB as intermediate storage',
          schedule_interval='0 15 * * *',
          catchup=False)

# Define the tasks using PythonOperator
extract_task = PythonOperator(
    task_id=MONGO_ETL_EXTRACT_COLLECTION,  # 'extract_data',
    python_callable=extract,
    dag=dag)

transform_task = PythonOperator(
    task_id=MONGO_ETL_TRANSFORM_COLLECTION,  # 'transform_data',
    python_callable=transform,
    dag=dag)

load_task = PythonOperator(
    task_id=MONGO_ETL_LOAD_COLLECTION,  # 'load_data',
    python_callable=load,
    dag=dag)

# Define task dependencies
extract_task >> transform_task >> load_task
