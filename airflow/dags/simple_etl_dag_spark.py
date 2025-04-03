from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils.spark import init_spark
from datetime import datetime
import os, pendulum

local_tz = pendulum.timezone("Europe/Athens")

# Define constants
INPUT_FILE = os.environ.get("INPUT_FILE")
MONGO_CONN_ID = os.environ.get("MONGO_CONN_ID")
MONGO_SPARK_ETL_DATABASE = os.environ.get("MONGO_SPARK_ETL_DATABASE")
MONGO_SPARK_EXTRACT_COLLECTION = os.environ.get(
    "MONGO_SPARK_EXTRACT_COLLECTION")
MONGO_SPARK_TRANSFORM_COLLECTION = os.environ.get(
    "MONGO_SPARK_TRANSFORM_COLLECTION")
MONGO_SPARK_LOAD_COLLECTION = os.environ.get("MONGO_SPARK_LOAD_COLLECTION")


# Task 1: Fetch large scale data from csv file
def spark_extract():
    spark_session = init_spark()
    # Read data from the iris.csv file using spark
    df = spark_session.read.options(delimiter=",",
                                    header=True,
                                    inferSchema=True).csv(INPUT_FILE)

    # Save the dataset in mongodb to be used in the next tasks
    df.write.format("mongodb").mode("overwrite").option(
        "database", MONGO_SPARK_ETL_DATABASE).option(
            "collection", MONGO_SPARK_EXTRACT_COLLECTION).save()


# Task 2: Data transformation
def spark_transform():
    # Start spark session
    spark_session = init_spark()

    # Fetch dataset from the previous task
    df = spark_session.read.format("mongodb").option(
        "database", MONGO_SPARK_ETL_DATABASE).option(
            "collection", MONGO_SPARK_EXTRACT_COLLECTION).load()

    # Example transformation: clean and filter data
    ndf = df.drop('extra', "Id")  # Example transformation

    # Save the transformed dataset to be used in the next task
    ndf.write.format("mongodb").mode("overwrite").option(
        "database", MONGO_SPARK_ETL_DATABASE).option(
            "collection", MONGO_SPARK_TRANSFORM_COLLECTION).save()


# Task 3: Train a machine learning model
def spark_load():
    spark_session = init_spark()
    # Fetch dataset from the previous task
    df = spark_session.read.format("mongodb").option(
        "database", MONGO_SPARK_ETL_DATABASE).option(
            "collection", MONGO_SPARK_TRANSFORM_COLLECTION).load()

    # Save the transformed dataset to be used in the next task
    df.write.format("mongodb").mode("overwrite").option(
        "database",
        MONGO_SPARK_ETL_DATABASE).option("collection",
                                         MONGO_SPARK_LOAD_COLLECTION).save()


# Define default arguments for DAG
default_args = {
    'owner': os.environ.get("MONGO_INITDB_ROOT_USERNAME"),
    'start_date': datetime(2025, 5, 1, tzinfo=local_tz),
}

# Define the DAG
dag = DAG(
    'simple_etl_spark_pipeline',
    default_args=default_args,
    description=
    'A simple ETL pipeline with three tasks that utilizes Spark for large scale data and Mongo as intermediate storage',
    schedule_interval='0 8 * * 1',
)

# Define the tasks using PythonOperator
fetch_data = PythonOperator(
    task_id=MONGO_SPARK_EXTRACT_COLLECTION,  #'spark_extract_data',
    python_callable=spark_extract,
    dag=dag,
)

transform_data = PythonOperator(
    task_id=MONGO_SPARK_TRANSFORM_COLLECTION,  #'spark_transform_data',
    python_callable=spark_transform,
    dag=dag,
)

train_model_task = PythonOperator(
    task_id=MONGO_SPARK_LOAD_COLLECTION,  #'spark_load_data',
    python_callable=spark_load,
    dag=dag,
)

# Define task dependencies
fetch_data >> transform_data >> train_model_task
