from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from sklearn import datasets
from sklearn.inspection import DecisionBoundaryDisplay
from sklearn.linear_model import LogisticRegression

import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature

from utils.spark import init_spark

default_args = {'owner': 'airflow'}

# Define the DAG
dag = DAG(
    'simple_ml_pipeline',
    default_args=default_args,
    description='A simple ML pipeline with three tasks',
    schedule_interval=None,
)


# Task 1: Fetch data from csv file
def fetch_data_from_file():
    spark_session = init_spark()
    # Read data from the iris.csv file using spark
    df = spark_session.read.options(
        delimiter=";", header=True,
        inferSchema=True).csv("/opt/airflow/dags/utils/iris.csv")
    # Save the dataset in mongodb to be used in the next tasks
    df.write.format("mongodb").mode("overwrite").option(
        "database", "aidapt").option("collection",
                                     "fetch_data_from_file").save()


# Task 2: Data transformation
def remove_columns():
    # Start spark session
    spark_session = init_spark()

    # Fetch dataset from the previous task
    df = spark_session.read.format("mongodb").option(
        "database", "aidapt").option("collection",
                                     "fetch_data_from_file").load()

    # Example transformation: clean and filter data
    ndf = df.drop('extra', "_id")  # Example cleaning

    # Save the transformed dataset to be used in the next task
    ndf.write.format("mongodb").mode("overwrite").option(
        "database", "aidapt").option("collection", "remove_columns").save()


# Task 3: Train a machine learning model
def train_model():
    # Use the fluent API to set the tracking uri and the active experiment
    mlflow.set_tracking_uri("http://mlflow:5000")
    # Sets the current active experiment to the "Iris_Models" experiment and returns the Experiment metadata
    iris_experiment = mlflow.set_experiment("Iris_Models")
    # Define a run name for this iteration of training.
    # If this is not set, a unique name will be auto-generated for your run.
    run_name = "iris_logreg_test"
    # Define an artifact path that the model will be saved to.
    artifact_path = "lr_iris"
    # Start spark session
    spark_session = init_spark()
    # Fetch dataset from the previous task
    df = spark_session.read.format("mongodb").option(
        "database", "aidapt").option("collection", "remove_columns").load()

    ndf = df.drop("_id")

    pdf = ndf.toPandas()

    X = pdf.iloc[:, :2]  # we only take the first two features.
    Y = pdf.iloc[:, -1]

    params = {"C": 1e5}

    logreg = LogisticRegression(**params)
    logreg.fit(X, Y)

    predicted = logreg.predict(X)
    score = logreg.score(X, Y)

    signature = infer_signature(X, predicted)

    # Initiate the MLflow run context
    with mlflow.start_run(run_name=run_name) as run:
        # Log the parameters used for the model fit
        mlflow.log_params(params)
        # Log the metrics (score)
        mlflow.log_metrics({"score": score})

        # Log the sklearn model and register as version 1
        mlflow.sklearn.log_model(
            sk_model=logreg,
            signature=signature,
            input_example=X,
            artifact_path=artifact_path,
            registered_model_name="sk-learn-log-reg-model")


# Define the tasks using PythonOperator
fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data_from_file,
    provide_context=True,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=remove_columns,
    provide_context=True,
    dag=dag,
)

train_model_task = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
fetch_data >> transform_data >> train_model_task
