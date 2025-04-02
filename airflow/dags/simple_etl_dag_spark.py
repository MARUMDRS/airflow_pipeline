from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils.spark import init_spark

# Define constants
INPUT_FILE = '/opt/airflow/dags/utils/iris.csv'
MONGO_CONN_ID = 'mongo_default'
MONGO_DB = 'seminar'
MONGO_EXTRACT_COLLECTION = 'etl_extract'
MONGO_TRANSFORM_COLLECTION = 'etl_transform'
MONGO_DATABASE = 'seminar'

default_args = {'owner': 'airflow'}

# Define the DAG
dag = DAG(
    'simple_etl_spark_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline with three tasks that utilizes spark',
    schedule_interval=None,
)


# Task 1: Fetch data from csv file

def spark_extract():
    spark_session = init_spark()
    # Read data from the iris.csv file using spark
    df = spark_session.read.options(
        delimiter=",", header=True,
        inferSchema=True).csv(INPUT_FILE)
    
    # Save the dataset in mongodb to be used in the next tasks
    df.write.format("mongodb").mode("overwrite").option(
        "database", MONGO_DATABASE).option("collection",
                                      "extract_spark").save()


# Task 2: Data transformation
def spark_transform():
    # Start spark session
    spark_session = init_spark()

    # Fetch dataset from the previous task
    df = spark_session.read.format("mongodb").option(
        "database", MONGO_DATABASE).option("collection",
                                     "extract_spark").load()

    # Example transformation: clean and filter data
    ndf = df.drop('extra', "Id")  # Example cleaning

    # Save the transformed dataset to be used in the next task
    ndf.write.format("mongodb").mode("overwrite").option(
        "database", MONGO_DATABASE).option("collection", "transform_spark").save()


# Task 3: Train a machine learning model
def spark_load():
    spark_session = init_spark()
    # Fetch dataset from the previous task
    df = spark_session.read.format("mongodb").option(
        "database", MONGO_DATABASE).option("collection", "transform_spark").load()
    
    
    # Save the transformed dataset to be used in the next task
    df.write.format("mongodb").mode("overwrite").option(
        "database", MONGO_DATABASE).option("collection", "load_spark").save()


# Define the tasks using PythonOperator
fetch_data = PythonOperator(
    task_id='spark_extract',
    python_callable=spark_extract,
    provide_context=True,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='spark_transform',
    python_callable=spark_transform,
    provide_context=True,
    dag=dag,
)

train_model_task = PythonOperator(
    task_id='spark_load',
    python_callable=spark_load,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
fetch_data >> transform_data >> train_model_task
