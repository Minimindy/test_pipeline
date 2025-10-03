# Test DAG by printing Hello World every 2 minutes
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

with DAG(
    dag_id="hello_world",
    start_date=datetime(2025, 10, 2),
    schedule="*/2 * * * *" ,
    catchup=False,
) as dag:

    hello_task = KubernetesPodOperator(
        task_id="hello",
        name="hello",
        namespace="airflow",
        image="alpine:latest",
        cmds=["echo", "Hello World"],
        get_logs=True,
        is_delete_operator_pod=True
    )