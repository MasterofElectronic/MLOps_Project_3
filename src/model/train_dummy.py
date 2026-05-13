import os
import mlflow
import mlflow.sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.datasets import make_classification
from mlflow.tracking import MlflowClient

MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-svc.mlflow.svc.cluster.local:5000")
MODEL_NAME = "diabetes-champion"

def main():
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("diabetes-readmission")
    client = MlflowClient(tracking_uri=MLFLOW_URI)

    X, y = make_classification(n_samples=100, n_features=20, random_state=42)
    model = LogisticRegression()
    model.fit(X, y)

    with mlflow.start_run(run_name="dummy_champion") as run:
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
        )
        best_run_id = run.info.run_id

    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    best_version = next(
        (v for v in sorted(versions, key=lambda x: int(x.version), reverse=True)
         if v.run_id == best_run_id),
        None
    )

    if best_version:
        client.set_registered_model_alias(
            name=MODEL_NAME,
            alias="champion",
            version=best_version.version,
        )
        print(f"Registered dummy champion v{best_version.version}")

if __name__ == "__main__":
    main()
