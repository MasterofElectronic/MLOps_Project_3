# MLOps Proyecto 2 — Diabetes Readmission Pipeline

**Pontificia Universidad Javeriana — Maestría en Inteligencia Artifical**

**Estudiantes:**

* **Juan Navas**
* **Camila Cuellar**
* **Jhonathan Murcia**

**Curso:** Operaciones de Machine Learning

---

## Descripción

Sistema MLOps completo desplegado en Kubernetes que implementa el ciclo de vida de un modelo de Machine Learning para predecir readmisión hospitalaria temprana (<30 días) en pacientes diabéticos. El dataset utilizado corresponde a 10 años (1999-2008) de atención clínica en 130 hospitales de EE.UU. con más de 100.000 registros.

---

## Arquitectura

```
Archivo CSV → Airflow DAG → PostgreSQL (raw/clean) → MLflow → API FastAPI → Streamlit
                                                          ↓
                                                       MinIO
                                                          ↓
                                              Prometheus → Grafana
                                                          ↑
                                                        Locust
```

### Componentes

| Componente        | Tecnología          | Namespace |
| ----------------- | -------------------- | --------- |
| Orquestación     | Apache Airflow 2.9.1 | airflow   |
| Base de datos     | PostgreSQL 15        | ingesta   |
| Object storage    | MinIO                | ingesta   |
| ML Tracking       | MLflow 2.22.0        | mlflow    |
| API de inferencia | FastAPI + Uvicorn    | inference |
| Interfaz gráfica | Streamlit            | inference |
| Pruebas de carga  | Locust 2.24.0        | inference |
| Métricas         | Prometheus + Grafana | mlops     |

---

## Requisitos previos

- Rocky Linux 9.x
- k3s v1.34+ instalado
- Helm v4+

---

## Despliegue

### 1. Clonar el repositorio

```bash
git clone https://github.com/masterofelectronic/mlops-proyecto2.git
cd mlops-proyecto2
```

### 2. Crear namespaces

```bash
kubectl create namespace ingesta
kubectl create namespace airflow
kubectl create namespace mlflow
kubectl create namespace inference
kubectl create namespace mlops
```

### 3. Aplicar secrets (Cross-Namespace)

Debido a que usamos diferentes namespaces, debemos aplicar los secretos en cada uno de ellos (asegrate de que el archivo `k8s/secrets-*.yaml` est configurado con el namespace correcto):

```bash
kubectl apply -f k8s/secrets-ingesta.yaml
kubectl apply -f k8s/secrets-airflow.yaml
kubectl apply -f k8s/secrets-mlflow.yaml
kubectl apply -f k8s/secrets-inference.yaml
```

### 4. Desplegar PostgreSQL y MinIO (Namespace: ingesta)

```bash
# PostgreSQL
kubectl apply -f k8s/postgres/configmap.yaml
kubectl apply -f k8s/postgres/pvc.yaml
kubectl apply -f k8s/postgres/statefulset.yaml
kubectl apply -f k8s/postgres/service.yaml
kubectl rollout status statefulset/postgres -n ingesta --timeout=120s

# MinIO
kubectl apply -f k8s/minio/pvc.yaml
kubectl apply -f k8s/minio/deployment.yaml
kubectl apply -f k8s/minio/service.yaml
kubectl rollout status deployment/minio -n ingesta --timeout=120s
kubectl apply -f k8s/minio/job-create-bucket.yaml
```

### 5. Desplegar MLflow con Helm (Namespace: mlflow)

```bash
helm upgrade --install mlflow k8s/mlflow-chart --namespace mlflow
```

### 6. Desplegar Airflow con Helm (Namespace: airflow)

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --values k8s/airflow/helm-values.yaml
```

### 7. Copiar DAG y dataset al pod de Airflow

**Para Windows (PowerShell):**

```powershell
$SCHEDULER = (kubectl get pod -n airflow -l component=scheduler -o jsonpath='{.items[0].metadata.name}')

# Dataset
kubectl cp data/Diabetes.csv airflow/${SCHEDULER}:/opt/airflow/dags/Diabetes.csv

# DAG
kubectl cp dags/diabetes_pipeline.py airflow/${SCHEDULER}:/opt/airflow/dags/diabetes_pipeline.py
```

**Para Linux / macOS (Bash):**

```bash
SCHEDULER=$(kubectl get pod -n airflow -l component=scheduler -o jsonpath='{.items[0].metadata.name}')

# Dataset
kubectl cp data/Diabetes.csv airflow/$SCHEDULER:/opt/airflow/dags/Diabetes.csv

# DAG
kubectl cp dags/diabetes_pipeline.py airflow/$SCHEDULER:/opt/airflow/dags/diabetes_pipeline.py
```

### 8. Crear Airflow Connection para PostgreSQL

**Para Windows (PowerShell):**

```powershell
$SCHEDULER = (kubectl get pod -n airflow -l component=scheduler -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n airflow $SCHEDULER -- airflow connections add mlops_postgres --conn-type postgres --conn-host postgres-svc.ingesta.svc.cluster.local --conn-port 5432 --conn-login mlops --conn-password mlops2026 --conn-schema mlops
```

**Para Linux / macOS (Bash):**

```bash
SCHEDULER=$(kubectl get pod -n airflow -l component=scheduler -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n airflow $SCHEDULER -- \
  airflow connections add mlops_postgres \
  --conn-type postgres \
  --conn-host postgres-svc.ingesta.svc.cluster.local \
  --conn-port 5432 \
  --conn-login mlops \
  --conn-password mlops2026 \
  --conn-schema mlops
```

### 10. Entrenar Modelo Inicial (Opcional pero recomendado)

Si es la primera vez que despliegas y el DAG de Airflow aún no ha sido ejecutado, la API fallará al iniciar porque no encontrará el modelo `diabetes-champion` en MLflow. Puedes ejecutar este Job rápido para registrar un modelo "dummy" y permitir que la API inicie:

```bash
kubectl apply -f k8s/jobs/train_dummy_job.yaml
kubectl wait --for=condition=complete job/train-dummy-job -n inference --timeout=120s
```

### 11. Desplegar API de inferencia

```bash
kubectl apply -f k8s/api/deployment.yaml
kubectl apply -f k8s/api/service.yaml
kubectl rollout status deployment/diabetes-api -n inference --timeout=120s
```

### 12. Desplegar Streamlit

```bash
kubectl apply -f k8s/streamlit/deployment.yaml
kubectl apply -f k8s/streamlit/service.yaml
kubectl rollout status deployment/diabetes-ui -n inference --timeout=120s
```

### 13. Desplegar observabilidad

```bash
# Prometheus (Configurado para scraping cross-namespace)
kubectl apply -f k8s/observability/prometheus/rbac.yaml
kubectl apply -f k8s/observability/prometheus/configmap.yaml
kubectl apply -f k8s/observability/prometheus/deployment.yaml

# Grafana (Con provisionamiento automático de Dashboards y Datasources)
kubectl apply -f k8s/observability/grafana/configmap-dashboards.yaml
kubectl apply -f k8s/observability/grafana/configmap-providers.yaml
kubectl apply -f k8s/observability/grafana/configmap-datasources.yaml
kubectl apply -f k8s/observability/grafana/deployment.yaml

kubectl rollout status deployment/prometheus -n mlops --timeout=120s
kubectl rollout status deployment/grafana -n mlops --timeout=120s
```

### 14. Desplegar Locust

```bash
kubectl create configmap locust-config --from-file=locustfile.py=locust/locustfile.py -n inference

kubectl apply -f k8s/locust/deployment.yaml
kubectl rollout status deployment/locust -n inference --timeout=120s
```

### 15. Ejecutar Pruebas de Carga (Carga Escalonada)

Para simular una carga escalonada realista (Step Load), ejecuta el siguiente comando desde el pod de Locust o port-forwarding:

**Para Windows (PowerShell):**

```powershell
$LOCUST_POD = (kubectl get pod -n inference -l app=locust -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n inference $LOCUST_POD -- locust -f /locust/locustfile.py --host=http://diabetes-api-svc:8000 --headless -u 1000 -r 10 --run-time 10m --step-load --step-users 100 --step-time 1m
```

**Para Linux / macOS (Bash):**

```bash
LOCUST_POD=$(kubectl get pod -n inference -l app=locust -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n inference $LOCUST_POD -- locust -f /locust/locustfile.py --host=http://diabetes-api-svc:8000 --headless -u 1000 -r 10 --run-time 10m --step-load --step-users 100 --step-time 1m
```

---

## Acceso a los servicios

> Accesibles desde la red universitaria en `http://10.43.101.82:<puerto>`
> Fuera de la red: configurar SSH tunnel via VPN universitaria

| Servicio      | NodePort | URL local              |
| ------------- | -------- | ---------------------- |
| Airflow UI    | 30088    | http://localhost:30088 |
| MLflow UI     | 30500    | http://localhost:30500 |
| MinIO Console | 30900    | http://localhost:30900 |
| FastAPI       | 30800    | http://localhost:30800 |
| Streamlit     | 30801    | http://localhost:30801 |
| Prometheus    | 30909    | http://localhost:30909 |
| Grafana       | 30300    | http://localhost:30300 |
| Locust        | 30089    | http://localhost:30089 |

---

## Solución de Problemas (Troubleshooting)

### Acceso Local via Port-Forward

Si el acceso via `NodePort` (localhost:puerto) no funciona en tu entorno local, utiliza los túneles directos de Kubernetes:

```powershell
# Airflow UI
kubectl port-forward -n airflow svc/airflow-webserver 30088:8080

# API de Inferencia & Streamlit
kubectl port-forward -n inference svc/diabetes-api-svc 30800:8000
kubectl port-forward -n inference svc/diabetes-ui-svc 30801:8501

# MLflow & MinIO Console
kubectl port-forward -n mlflow svc/mlflow-svc 30500:5000
kubectl port-forward -n ingesta svc/minio-svc 30900:9001

# Prometheus, Grafana & Locust
kubectl port-forward -n mlops svc/prometheus-svc 30909:9090
kubectl port-forward -n mlops svc/grafana-svc 30300:3000
kubectl port-forward -n inference svc/locust-svc 30089:8089
```

### Reset de Contraseña Airflow

Si la contraseña por defecto (`admin2026`) no funciona, puedes forzar el reset con:

```bash
kubectl exec -n airflow airflow-scheduler-0 -- \
  airflow users reset-password --username admin --password admin2026
```

## Credenciales

| Servicio   | Usuario    | Contraseña        |
| ---------- | ---------- | ------------------ |
| Airflow    | admin      | admin2026          |
| MLflow     | —         | sin autenticación |
| MinIO      | minioadmin | minioadmin2026     |
| Grafana    | admin      | admin2026          |
| PostgreSQL | mlops      | mlops2026          |

---

## Pipeline de datos

El DAG `diabetes_pipeline` implementa carga incremental en lotes de máximo 15.000 registros:

| Tarea                     | Descripción                                                                |
| ------------------------- | --------------------------------------------------------------------------- |
| `validate_source`       | Verifica existencia y estructura del archivo CSV                            |
| `load_batch_to_raw`     | Carga el siguiente lote a `raw_diabetes` con row_hash para deduplicación |
| `validate_data_quality` | Valida calidad del lote: nulos, rangos, valores válidos                    |
| `process_and_clean`     | Limpieza, feature engineering y split 72/13/15                              |
| `train_and_register`    | Entrena LR + RF + XGBoost, registra en MLflow, promueve champion            |

### Dataset — 101.766 registros — 7 lotes

| Lote | Registros  | Estado      |
| ---- | ---------- | ----------- |
| 1-6  | 15.000 c/u | ✅ Cargados |
| 7    | 6.766      | ✅ Cargado  |

### Métrica de selección de modelo

Se usa **ROC-AUC** como métrica principal porque el dataset está desbalanceado (~11% readmisión temprana). En un contexto clínico, la capacidad discriminativa del modelo es más relevante que el accuracy global. Un falso negativo (no detectar readmisión) tiene mayor costo clínico que un falso positivo.

### Resultados primer experimento (lote 1 — 15.000 registros)

| Modelo              | ROC-AUC | F1    |
| ------------------- | ------- | ----- |
| XGBoost             | mejor   | 0.796 |
| Random Forest       | —      | 0.839 |
| Logistic Regression | —      | 0.708 |

**Modelo productivo:** `diabetes-champion` (XGBoost) con alias `champion` en MLflow.

---

## API de inferencia

### Endpoints

| Endpoint        | Método | Descripción                               |
| --------------- | ------- | ------------------------------------------ |
| `/health`     | GET     | Estado de la API y modelo cargado          |
| `/predict`    | POST    | Predicción de readmisión                 |
| `/model-info` | GET     | Nombre, versión y alias del modelo activo |
| `/metrics`    | GET     | Métricas Prometheus                       |

### Ejemplo de predicción

```bash
curl -X POST http://localhost:30800/predict \
  -H "Content-Type: application/json" \
  -d '{
    "time_in_hospital": 5,
    "num_lab_procedures": 45,
    "num_procedures": 2,
    "num_medications": 15,
    "number_outpatient": 0,
    "number_emergency": 1,
    "number_inpatient": 2,
    "number_diagnoses": 7,
    "age_encoded": 6,
    "admission_type_encoded": 1,
    "discharge_encoded": 1,
    "admission_source_encoded": 1,
    "insulin_encoded": 2,
    "change_encoded": 1,
    "diabetesmed_encoded": 1,
    "a1cresult_encoded": 0,
    "max_glu_serum_encoded": 0,
    "num_medications_log": 2.77,
    "service_utilization": 3
  }'
```

---

## Observabilidad

### Dashboards Grafana

| Dashboard                 | Descripción                                                  |
| ------------------------- | ------------------------------------------------------------- |
| MLOps API Dashboard       | RPS, latencia, percentiles p50/p95/p99, errores, predicciones |
| MLOps Ingestion Dashboard | Registros raw/clean, lotes, distribución target, inferencias |

### Pruebas de carga — Locust

| Usuarios | RPS      | Latencia promedio | p95   | Errores |
| -------- | -------- | ----------------- | ----- | ------- |
| 5        | ~4 req/s | ~25ms             | ~38ms | 0%      |
| 10       | ~8 req/s | ~24ms             | ~38ms | 0%      |

La API mantiene latencias estables al duplicar la carga. El límite operativo no fue alcanzado bajo las condiciones de recursos configuradas (1 CPU / 1Gi RAM).

---

## Estructura del repositorio

```
mlops-proyecto2/
├── k8s/
│   ├── namespace.yaml
│   ├── secrets.yaml
│   ├── postgres/
│   ├── minio/
│   ├── mlflow/
│   ├── airflow/
│   │   └── helm-values.yaml
│   ├── api/
│   ├── streamlit/
│   ├── locust/
│   └── observability/
│       ├── prometheus/
│       └── grafana/
├── dags/
│   └── diabetes_pipeline.py
├── src/
│   ├── api/
│   │   ├── main.py
│   │   └── requirements.txt
│   └── ui/
│       ├── app.py
│       └── requirements.txt
├── docker/
│   ├── mlflow/Dockerfile
│   ├── api/Dockerfile
│   └── streamlit/Dockerfile
├── locust/
│   └── locustfile.py
├── migrations/
└── README.md
```

---

## Imágenes Docker

| Imagen                                 | Tag     | Descripción                |
| -------------------------------------- | ------- | --------------------------- |
| `masterofelectronic/mlflow-postgres` | v2.22.0 | MLflow con psycopg2 + boto3 |
| `masterofelectronic/diabetes-api`    | 1.0.1   | FastAPI de inferencia       |
| `masterofelectronic/diabetes-ui`     | 1.0.0   | Streamlit UI                |
| `apache/airflow`                     | 2.9.1   | Airflow estable             |

---

## Video de sustentación

[YouTube — MLOps Proyecto 2](https://youtu.be/PENDING)
