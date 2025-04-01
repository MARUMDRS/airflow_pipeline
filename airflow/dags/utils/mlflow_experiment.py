import mlflow
from mlflow.models import infer_signature
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import json

def save_confusion_matrix(cm, output_path):
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")
    plt.ylabel("Actual")
    plt.xlabel("Predicted")
    plt.title("Confusion Matrix")
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

def main():
    # Set MLflow tracking URI and experiment
    mlflow.set_tracking_uri("http://localhost:1313")
    mlflow.set_experiment("mlflow_tutorial")

    # 1. Load the Iris dataset and convert to a DataFrame
    iris = load_iris()
    iris_df = pd.DataFrame(iris.data, columns=iris.feature_names)
    iris_df["target"] = iris.target
    print("DataFrame head:")
    print(iris_df.head())

    # 2. Split the DataFrame into training and testing sets
    train_df, test_df = train_test_split(iris_df, test_size=0.2, random_state=42)
    X_train = train_df.drop("target", axis=1)
    y_train = train_df["target"]
    X_test = test_df.drop("target", axis=1)
    y_test = test_df["target"]

    # Save a sample of the training DataFrame as CSV for artifact logging
    train_csv = "train_sample.csv"
    train_df.head(10).to_csv(train_csv, index=False)

    # 3. Start an MLflow run
    with mlflow.start_run(log_system_metrics=True) as run:
        # Log dataset using MLflow's dataset module
        mlflow.log_artifact(train_csv, artifact_path="dataset_samples")
        
        # 4. Hyperparameter tuning using GridSearchCV on DataFrame data
        param_grid = {
            "n_estimators": [50, 100],
            "max_depth": [3, 5],
            "random_state": [42]
        }
        grid_search = GridSearchCV(RandomForestClassifier(), param_grid, cv=3, scoring='accuracy')
        grid_search.fit(X_train, y_train)
        
        # Create a child run for each hyperparameter candidate from cv_results_
        for idx, params in enumerate(grid_search.cv_results_["params"]):
            with mlflow.start_run(run_name=f"Child_Run_CV_{idx}", nested=True, log_system_metrics=True):
                mlflow.log_params(params)
                mlflow.log_metric("mean_test_score", grid_search.cv_results_["mean_test_score"][idx])
                mlflow.log_metric("std_test_score", grid_search.cv_results_["std_test_score"][idx])
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
            targets="target",
            model_type="classifier",
            # evaluators=["default"],
        )
        
        # 8. Set additional metadata tags
        mlflow.set_tag("model_type", "RandomForestClassifier")
        mlflow.set_tag("dataset", "iris")
        
        print(f"Test Accuracy: {acc:.4f}")
        print(f"Best Parameters: {best_params}")

if __name__ == "__main__":
    main()
