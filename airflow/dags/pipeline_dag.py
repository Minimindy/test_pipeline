from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.models.baseoperator import chain

NAMESPACE = "mlops-lab"
IMAGE = "textpipe-runner:0.1.0"
SERVICE_ACCOUNT = "default"
DATA_BUCKET = "data"
ARTIFACTS_BUCKET = "artifacts"

RAW_S3_URI = "{{ var.value.RAW_S3_URI | default('s3://data/raw/textclf_sample.csv') }}"
CLEAN_URI = f"s3://{DATA_BUCKET}/clean/{{{{ ds_nodash }}}}/dataset.parquet"
PREV_CLEAN_URI = f"s3://{DATA_BUCKET}/clean/{{{{ prev_ds_nodash }}}}/dataset.parquet"

secrets = [
    Secret("env", "S3_ENDPOINT_URL", "textpipe-secrets", "S3_ENDPOINT_URL"),
    Secret("env", "AWS_ACCESS_KEY_ID", "textpipe-secrets", "AWS_ACCESS_KEY_ID"),
    Secret("env", "AWS_SECRET_ACCESS_KEY", "textpipe-secrets", "AWS_SECRET_ACCESS_KEY"),
]

default_args = {
    "retries": 1,
}

with DAG(
    dag_id="text_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 10, 2),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    process = KubernetesPodOperator(
        task_id="process",
        namespace=NAMESPACE,
        image=IMAGE,
        cmds=["python", "-m", "pipelines.process"],
        arguments=[
            "--input", RAW_S3_URI,
            "--output", CLEAN_URI,
            "--artifacts-bucket", ARTIFACTS_BUCKET,
        ],
        secrets=secrets,
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name=SERVICE_ACCOUNT,
    )

    validate = KubernetesPodOperator(
        task_id="validate",
        namespace=NAMESPACE,
        image=IMAGE,
        cmds=["python", "-m", "pipelines.validate"],
        arguments=[
            "--input", CLEAN_URI,
            "--min-rows", "5",
            "--min-classes", "2",
        ],
        secrets=secrets,
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name=SERVICE_ACCOUNT,
    )

    monitor = KubernetesPodOperator(
        task_id="monitor",
        namespace=NAMESPACE,
        image=IMAGE,
        cmds=["python", "-m", "pipelines.monitor"],
        arguments=[
            "--reference", PREV_CLEAN_URI,
            "--current", CLEAN_URI,
            "--artifacts-bucket", ARTIFACTS_BUCKET,
        ],
        secrets=secrets,
        get_logs=True,
        is_delete_operator_pod=True,
        service_account_name=SERVICE_ACCOUNT,
    )

    chain(process, validate, monitor)
