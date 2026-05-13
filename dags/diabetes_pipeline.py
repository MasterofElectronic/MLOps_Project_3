import os
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, f1_score, recall_score, precision_score
from xgboost import XGBClassifier

# ── Configuración global ──────────────────────────────────────────────────────

DATA_FILE     = "/opt/airflow/dags/Diabetes.csv"
BATCH_SIZE    = 15000
EXPERIMENT    = "diabetes-readmission"
MODEL_NAME    = "diabetes-champion"
MLFLOW_URI    = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-svc:5000")
DB_CONN       = "mlops_postgres"   # Airflow connection id — lo creamos abajo

DEFAULT_ARGS = {
    "owner": "mlops",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_pg():
    """Devuelve un hook de PostgreSQL usando la connection de Airflow."""
    return PostgresHook(postgres_conn_id=DB_CONN)

def row_hash(row: pd.Series) -> str:
    """Hash SHA256 de una fila para detectar duplicados."""
    return hashlib.sha256(row.to_json().encode()).hexdigest()

# ── Tasks ─────────────────────────────────────────────────────────────────────

def validate_source(**ctx):
    """Verifica que el archivo fuente exista y no esté vacío."""
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Dataset no encontrado: {DATA_FILE}")
    size = os.path.getsize(DATA_FILE)
    if size < 1000:
        raise ValueError(f"Archivo demasiado pequeño ({size} bytes) — puede estar corrupto")
    df = pd.read_csv(DATA_FILE, nrows=1)
    required = {"encounter_id", "patient_nbr", "readmitted"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Columnas faltantes: {missing}")
    print(f" Archivo validado: {DATA_FILE} ({size/1e6:.1f} MB)")

def load_batch_to_raw(**ctx):
    """
    Carga incremental por lotes de máximo 15.000 registros.
    Determina el offset leyendo cuántos registros ya existen en raw_diabetes.
    Idempotente: usa row_hash para evitar duplicados.
    """
    hook = get_pg()
    conn = hook.get_conn()
    cur  = conn.cursor()

    # Cuántos registros hay ya cargados
    cur.execute("SELECT COUNT(*) FROM raw_diabetes;")
    already_loaded = cur.fetchone()[0]

    # Leer el lote siguiente
    df = pd.read_csv(DATA_FILE, skiprows=range(1, already_loaded + 1),
                     nrows=BATCH_SIZE)

    if df.empty:
        print("Todos los registros ya fueron cargados.")
        ctx["ti"].xcom_push(key="batch_id", value=None)
        cur.close(); conn.close()
        return
    
    batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    source    = os.path.basename(DATA_FILE)
    loaded    = 0
    skipped   = 0

    for _, row in df.iterrows():
        rh = row_hash(row)
        try:
            cur.execute("""
                INSERT INTO raw_diabetes (
                    batch_id, source_file, row_hash, status,
                    encounter_id, patient_nbr, race, gender, age, weight,
                    admission_type_id, discharge_disposition_id, admission_source_id,
                    time_in_hospital, payer_code, medical_specialty,
                    num_lab_procedures, num_procedures, num_medications,
                    number_outpatient, number_emergency, number_inpatient,
                    diag_1, diag_2, diag_3, number_diagnoses,
                    max_glu_serum, a1cresult, metformin, repaglinide,
                    nateglinide, chlorpropamide, glimepiride, acetohexamide,
                    glipizide, glyburide, tolbutamide, pioglitazone,
                    rosiglitazone, acarbose, miglitol, troglitazone,
                    tolazamide, examide, citoglipton, insulin,
                    glyburide_metformin, glipizide_metformin,
                    glimepiride_pioglitazone, metformin_rosiglitazone,
                    metformin_pioglitazone, change, diabetesmed, readmitted
                ) VALUES (
                    %s,%s,%s,'loaded',
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
            """, (
                batch_id, source, rh,
                row.get("encounter_id"), row.get("patient_nbr"),
                row.get("race"), row.get("gender"), row.get("age"),
                row.get("weight"), row.get("admission_type_id"),
                row.get("discharge_disposition_id"), row.get("admission_source_id"),
                row.get("time_in_hospital"), row.get("payer_code"),
                row.get("medical_specialty"), row.get("num_lab_procedures"),
                row.get("num_procedures"), row.get("num_medications"),
                row.get("number_outpatient"), row.get("number_emergency"),
                row.get("number_inpatient"), row.get("diag_1"),
                row.get("diag_2"), row.get("diag_3"),
                row.get("number_diagnoses"), row.get("max_glu_serum"),
                row.get("a1cresult"), row.get("metformin"),
                row.get("repaglinide"), row.get("nateglinide"),
                row.get("chlorpropamide"), row.get("glimepiride"),
                row.get("acetohexamide"), row.get("glipizide"),
                row.get("glyburide"), row.get("tolbutamide"),
                row.get("pioglitazone"), row.get("rosiglitazone"),
                row.get("acarbose"), row.get("miglitol"),
                row.get("troglitazone"), row.get("tolazamide"),
                row.get("examide"), row.get("citoglipton"),
                row.get("insulin"), row.get("glyburide_metformin"),
                row.get("glipizide_metformin"), row.get("glimepiride_pioglitazone"),
                row.get("metformin_rosiglitazone"), row.get("metformin_pioglitazone"),
                row.get("change"), row.get("diabetesmed"), row.get("readmitted"),
            ))
            loaded += 1
        except Exception:
            skipped += 1  # duplicado por row_hash UNIQUE

    conn.commit()
    cur.close(); conn.close()

    print(f"Lote {batch_id}: {loaded} cargados, {skipped} duplicados omitidos")
    ctx["ti"].xcom_push(key="batch_id", value=batch_id)

def validate_data_quality(**ctx):
    """Validaciones básicas sobre el lote recién cargado."""
    batch_id = ctx["ti"].xcom_pull(key="batch_id", task_ids="load_batch_to_raw")
    if not batch_id:
        print("Sin lote nuevo — skip")
        return
    
    hook = get_pg()
    df = hook.get_pandas_df(
        f"SELECT * FROM raw_diabetes WHERE batch_id = '{batch_id}'"
    )

    checks = {
        "registros > 0": len(df) > 0,
        "readmitted no nulo": df["readmitted"].notna().all(),
        "time_in_hospital >= 1":    (df["time_in_hospital"] >= 1).all(),
        "gender válido":            df["gender"].isin(["Male","Female","Unknown/Invalid"]).all(),
    }

    failed = [k for k, v in checks.items() if not v]
    if failed:
        raise ValueError(f"Validaciones fallidas: {failed}")
    
    print(f"Calidad OK para {batch_id}: {len(df)} registros, {checks}")

def process_and_clean(**ctx):
    """
    Limpieza y feature engineering sobre el lote crudo.
    Estrategia:
    - Eliminar columnas con >50% nulos o sin varianza útil
    - Codificar categóricas con LabelEncoder
    - Target: readmitted → 1 si '<30', 0 en caso contrario
    - Feature derivada: service_utilization = outpatient + emergency + inpatient
    """
    batch_id = ctx["ti"].xcom_pull(key="batch_id", task_ids="load_batch_to_raw")
    if not batch_id:
        print("Sin lote nuevo — skip")
        return

    hook = get_pg()
    df = hook.get_pandas_df(
        f"SELECT * FROM raw_diabetes WHERE batch_id = '{batch_id}'"
    )
    raw_ids = df["id"].tolist()

    # ── Target ────────────────────────────────────────────────────────────────
    df["readmitted_binary"] = (df["readmitted"] == "<30").astype(int)

    # ── Eliminar columnas no útiles ───────────────────────────────────────────
    drop_cols = [
        "weight",        # >95% nulos
        "payer_code",    # >40% nulos, no predictivo
        "medical_specialty",  # >50% nulos
        "encounter_id", "patient_nbr",  # identificadores
        "diag_1", "diag_2", "diag_3",  # requieren codificación ICD compleja
        "readmitted",    # reemplazada por target
        # columnas de metadata interna
        "id", "batch_id", "load_timestamp", "source_file",
        "row_hash", "status",
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    # ── Edad: convertir rango a ordinal ───────────────────────────────────────
    age_map = {
        "[0-10)":0,"[10-20)":1,"[20-30)":2,"[30-40)":3,"[40-50)":4,
        "[50-60)":5,"[60-70)":6,"[70-80)":7,"[80-90)":8,"[90-100)":9
    }
    df["age_encoded"] = df["age"].map(age_map).fillna(4).astype(int)

    # ── Categóricas simples ───────────────────────────────────────────────────
    le = LabelEncoder()
    cat_cols = [
        "race", "gender", "max_glu_serum", "a1cresult",
        "metformin", "repaglinide", "nateglinide", "chlorpropamide",
        "glimepiride", "acetohexamide", "glipizide", "glyburide",
        "tolbutamide", "pioglitazone", "rosiglitazone", "acarbose",
        "miglitol", "troglitazone", "tolazamide", "examide",
        "citoglipton", "insulin", "glyburide_metformin",
        "glipizide_metformin", "glimepiride_pioglitazone",
        "metformin_rosiglitazone", "metformin_pioglitazone",
        "change", "diabetesmed",
    ]
    for col in cat_cols:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")
            df[col + "_encoded"] = le.fit_transform(df[col].astype(str))

    # ── Features numéricas ────────────────────────────────────────────────────
    df["num_medications_log"]  = np.log1p(df["num_medications"].fillna(0))
    df["service_utilization"]  = (
        df["number_outpatient"].fillna(0) +
        df["number_emergency"].fillna(0) +
        df["number_inpatient"].fillna(0)
    )

    # ── Split ─────────────────────────────────────────────────────────────────
    df["dataset_split"] = "train"
    idx = df.index.tolist()
    _, test_idx = train_test_split(idx, test_size=0.15, random_state=42)
    _, val_idx  = train_test_split(
        [i for i in idx if i not in test_idx], test_size=0.15, random_state=42
    )
    df.loc[val_idx,  "dataset_split"] = "val"
    df.loc[test_idx, "dataset_split"] = "test"

    # ── Insertar en clean_diabetes ────────────────────────────────────────────
    conn = hook.get_conn()
    cur  = conn.cursor()

    for pos, (_, row) in enumerate(df.iterrows()):
        cur.execute("""
            INSERT INTO clean_diabetes (
                raw_id, batch_id, time_in_hospital, num_lab_procedures,
                num_procedures, num_medications, number_outpatient,
                number_emergency, number_inpatient, number_diagnoses,
                age_encoded, admission_type_encoded, discharge_encoded,
                admission_source_encoded, insulin_encoded, change_encoded,
                diabetesmed_encoded, a1cresult_encoded, max_glu_serum_encoded,
                num_medications_log, service_utilization, readmitted_binary,
                dataset_split
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
        """, (
            raw_ids[pos] if pos < len(raw_ids) else None,
            batch_id,
            row.get("time_in_hospital"),
            row.get("num_lab_procedures"),
            row.get("num_procedures"),
            row.get("num_medications"),
            row.get("number_outpatient"),
            row.get("number_emergency"),
            row.get("number_inpatient"),
            row.get("number_diagnoses"),
            row.get("age_encoded"),
            row.get("admission_type_id"),
            row.get("discharge_disposition_id"),
            row.get("admission_source_id"),
            row.get("insulin_encoded"),
            row.get("change_encoded"),
            row.get("diabetesmed_encoded"),
            row.get("a1cresult_encoded"),
            row.get("max_glu_serum_encoded"),
            row.get("num_medications_log"),
            row.get("service_utilization"),
            int(row["readmitted_binary"]),
            row["dataset_split"],
        ))

    conn.commit()
    cur.close(); conn.close()
    print(f"Clean data insertada para {batch_id}: {len(df)} registros")

def train_and_register(**ctx):
    """
    Entrena LR, RF y XGBoost sobre los datos limpios acumulados.
    Registra cada experimento en MLflow.
    Promueve el mejor modelo (ROC-AUC en val) con alias 'champion'.
    """
    batch_id = ctx["ti"].xcom_pull(key="batch_id", task_ids="load_batch_to_raw")

    hook = get_pg()

    # Usar TODOS los datos limpios acumulados hasta ahora
    train_df = hook.get_pandas_df(
        "SELECT * FROM clean_diabetes WHERE dataset_split = 'train'"
    )
    val_df = hook.get_pandas_df(
        "SELECT * FROM clean_diabetes WHERE dataset_split = 'val'"
    )

    feature_cols = [
        "time_in_hospital", "num_lab_procedures", "num_procedures",
        "num_medications", "number_outpatient", "number_emergency",
        "number_inpatient", "number_diagnoses", "age_encoded",
        "admission_type_encoded", "discharge_encoded",
        "admission_source_encoded", "insulin_encoded", "change_encoded",
        "diabetesmed_encoded", "a1cresult_encoded", "max_glu_serum_encoded",
        "num_medications_log", "service_utilization",
    ]

    X_train = train_df[feature_cols].fillna(0)
    y_train = train_df["readmitted_binary"]
    X_val   = val_df[feature_cols].fillna(0)
    y_val   = val_df["readmitted_binary"]

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)
    client = MlflowClient(tracking_uri=MLFLOW_URI)

    models = {
        "logistic_regression": LogisticRegression(
            max_iter=1000, class_weight="balanced", random_state=42
        ),
        "random_forest": RandomForestClassifier(
            n_estimators=100, class_weight="balanced",
            n_jobs=-1, random_state=42
        ),
        "xgboost": XGBClassifier(
            n_estimators=100, scale_pos_weight=len(y_train[y_train==0])/max(1,len(y_train[y_train==1])),
            eval_metric="logloss", random_state=42, verbosity=0
        ),
    }

    best_auc   = -1
    best_run_id = None
    best_model_name = None

    for model_name, model in models.items():
        with mlflow.start_run(run_name=f"{model_name}_{batch_id}") as run:
            model.fit(X_train, y_train)

            y_pred_proba = model.predict_proba(X_val)[:, 1]
            y_pred       = model.predict(X_val)

            auc       = roc_auc_score(y_val, y_pred_proba)
            f1        = f1_score(y_val, y_pred, average="weighted")
            recall    = recall_score(y_val, y_pred, average="weighted")
            precision = precision_score(y_val, y_pred, average="weighted", zero_division=0)

            mlflow.log_params(model.get_params())
            mlflow.log_metrics({
                "roc_auc":   auc,
                "f1":        f1,
                "recall":    recall,
                "precision": precision,
                "train_size": len(X_train),
                "val_size":   len(X_val),
            })
            mlflow.log_param("batch_id", batch_id)
            mlflow.log_param("feature_count", len(feature_cols))

            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="model",
                registered_model_name=MODEL_NAME,
            )

            print(f"  {model_name}: ROC-AUC={auc:.4f} F1={f1:.4f}")

            if auc > best_auc:
                best_auc        = auc
                best_run_id     = run.info.run_id
                best_model_name = model_name
    
    # ── Promover el mejor modelo como 'champion' ──────────────────────────────
    # Obtener la versión recién registrada del mejor run
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
        print(f"Champion: {best_model_name} v{best_version.version} "
              f"(ROC-AUC={best_auc:.4f})")
    else:
        raise RuntimeError("No se pudo encontrar la versión del mejor modelo")

# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="diabetes_pipeline",
    description="Pipeline MLOps: ingesta incremental → limpieza → entrenamiento → MLflow",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "diabetes", "javeriana"],
) as dag:

    t1 = PythonOperator(task_id="validate_source",       python_callable=validate_source)
    t2 = PythonOperator(task_id="load_batch_to_raw",     python_callable=load_batch_to_raw)
    t3 = PythonOperator(task_id="validate_data_quality", python_callable=validate_data_quality)
    t4 = PythonOperator(task_id="process_and_clean",     python_callable=process_and_clean)
    t5 = PythonOperator(task_id="train_and_register",    python_callable=train_and_register)

    t1 >> t2 >> t3 >> t4 >> t5