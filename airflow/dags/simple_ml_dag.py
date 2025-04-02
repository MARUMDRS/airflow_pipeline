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
INPUT_FILE = os.environ.get("INPUT_FILE")
MLFLOW_URI = os.environ.get("MLFLOW_URI")
MLFLOW_EXPERIMENT_NAME = os.environ.get("MLFLOW_EXPERIMENT_NAME")
MONGO_CONN_ID = os.environ.get("MONGO_CONN_ID")
MONGO_ETL_LOAD_COLLECTION = os.environ.get("MONGO_ETL_LOAD_COLLECTION")

MONGO_ML_DATABASE = os.environ.get("MONGO_ML_DATABASE")
MONGO_ML_EXTRACT_COLLECTION = os.environ.get("MONGO_ML_EXTRACT_COLLECTION")
MONGO_ML_TRANSFORM_COLLECTION = os.environ.get(
    "MONGO_ML_TRANSFORM_COLLECTION")
MONGO_ML_TRAIN_COLLECTION = os.environ.get("MONGO_ML_TRAIN_COLLECTION")

default_args = {'owner': os.environ.get("MONGO_INITDB_ROOT_USERNAME")}

# Define the DAG
dag = DAG(
    'simple_ml_pipeline',
    default_args=default_args,
    description='A simple ML pipeline with three tasks',
    schedule_interval=None,
)


def save_confusion_matrix(cm, output_path):
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")
    plt.ylabel("Actual")
    plt.xlabel("Predicted")
    plt.title("Confusion Matrix")
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


def extract():
    df = pd.read_csv(INPUT_FILE)
    data = df.to_dict(orient='records')
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db = client[MONGO_ML_DATABASE]

    ml_collection = db[MONGO_ML_EXTRACT_COLLECTION]
    ml_collection.delete_many({})
    ml_collection.insert_many(data)


def preprocessing():
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db = client[MONGO_ML_DATABASE]

    load_collection = db[MONGO_ML_EXTRACT_COLLECTION]
    data = list(load_collection.find({}))

    df = pd.DataFrame(data)
    df = df.drop('Id', axis=1)  # Example cleaning

    transformed_data = df.to_dict(orient='records')
    
    preprocessed_collection = db[MONGO_ML_TRANSFORM_COLLECTION]
    preprocessed_collection.delete_many({})
    preprocessed_collection.insert_many(transformed_data)


# Task 3: Train a machine learning model
def train_model():
    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db = client[MONGO_ML_DATABASE]

    load_collection = db[MONGO_ML_TRANSFORM_COLLECTION]
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    # 1. Load the Iris dataset and convert to a DataFrame
    data = pd.DataFrame(list(load_collection.find({})),
                        columns=[
                            "SepalLengthCm", "SepalWidthCm", "PetalLengthCm",
                            "PetalWidthCm", "Species"
                        ])

    # data["Species"] = iris.target
    print("DataFrame head:")
    print(data.head())

    # 2. Split the DataFrame into training and testing sets
    train_df, test_df = train_test_split(data, test_size=0.2, random_state=42)
    X_train = train_df.drop("Species", axis=1)
    y_train = train_df["Species"]
    X_test = test_df.drop("Species", axis=1)
    y_test = test_df["Species"]

    # Save a sample of the training DataFrame as CSV for artifact logging
    train_csv = "train_sample.csv"
    train_df.head(10).to_csv(train_csv, index=False)
    run_name = "iris_run"
    # 3. Start an MLflow run
    with mlflow.start_run(log_system_metrics=True, run_name=run_name) as run:
        # Log dataset using MLflow's dataset module
        mlflow.log_artifact(train_csv, artifact_path="dataset_samples")

        # 4. Hyperparameter tuning using GridSearchCV on DataFrame data
        param_grid = {
            "n_estimators": [50, 100],
            "max_depth": [3, 5],
            "random_state": [42]
        }
        grid_search = GridSearchCV(RandomForestClassifier(),
                                   param_grid,
                                   cv=3,
                                   scoring='accuracy')
        grid_search.fit(X_train, y_train)

        # Create a child run for each hyperparameter candidate from cv_results_
        for idx, params in enumerate(grid_search.cv_results_["params"]):
            with mlflow.start_run(run_name=f"Child_Run_CV_{idx}",
                                  nested=True,
                                  log_system_metrics=True):
                mlflow.log_params(params)
                mlflow.log_metric(
                    "mean_test_score",
                    grid_search.cv_results_["mean_test_score"][idx])
                mlflow.log_metric(
                    "std_test_score",
                    grid_search.cv_results_["std_test_score"][idx])
                print(f"Logged CV result for candidate {idx}: {params}")

        best_model = grid_search.best_estimator_
        best_params = grid_search.best_params_
        mlflow.log_params(best_params)

        # 5. Evaluate the best model on the test set
        predictions = best_model.predict(X_test)
        acc = accuracy_score(y_test, predictions)
        mlflow.log_metric("accuracy", acc)

        # Log a confusion matrix artifact
        cm = confusion_matrix(y_test, predictions)
        cm_path = "confusion_matrix.png"
        save_confusion_matrix(cm, cm_path)
        mlflow.log_artifact(cm_path, artifact_path="evaluation")

        # Log a classification report artifact
        report = classification_report(y_test, predictions, output_dict=True)
        report_path = "classification_report.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=4)
        mlflow.log_artifact(report_path, artifact_path="evaluation")

        # 6. Log the model with signature and input example
        signature = infer_signature(X_test, best_model.predict(X_test))
        mlflow.sklearn.log_model(
            sk_model=best_model,
            artifact_path="iris_rf_model",
            signature=signature,
            input_example=X_test,  # Example input for the model
            # registered_model_name="iris_rf_model"  # Uncomment to register the model
        )
        model_uri = mlflow.get_artifact_uri("iris_rf_model")

        # 7. Automatic Evaluation using mlflow.evaluate
        eval_results = mlflow.evaluate(
            model=model_uri,
            data=test_df,
            targets="Species",
            model_type="classifier",
            # evaluators=["default"],
        )

        # 8. Set additional metadata tags
        mlflow.set_tag("model_type", "RandomForestClassifier")
        mlflow.set_tag("dataset", "iris")

        print(f"Test Accuracy: {acc:.4f}")
        print(f"Best Parameters: {best_params}")


# Define the tasks using PythonOperator
extract_task = PythonOperator(
    task_id=MONGO_ML_EXTRACT_COLLECTION,
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

preprocessing_task = PythonOperator(
    task_id=MONGO_ML_TRANSFORM_COLLECTION,
    python_callable=preprocessing,
    provide_context=True,
    dag=dag,
)

train_model_task = PythonOperator(
    task_id=MONGO_ML_TRAIN_COLLECTION,
    python_callable=train_model,
    provide_context=True,
    dag=dag,
)

extract_task >> preprocessing_task >> train_model_task
