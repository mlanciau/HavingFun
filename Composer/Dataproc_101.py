import airflow
from airflow import DAG
from airflow import models
from airflow.models import Variable

from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator

from datetime import timedelta
from pprint import pprint

Variable.set("task_list", [1, 2, 3], serialize_json=True)

default_dag_args = {
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with models.DAG('Dataproc_101',
    schedule_interval=None,
    description='Used for testing security / service account for Dataproc Operator',
    catchup=False,
    default_args=default_dag_args) as dag:

    def print_context(ds, **kwargs):
        pprint(kwargs)
        print(ds)
        return Variable.get("task_list", deserialize_json=True)

    create_dataproc_task = DataprocClusterCreateOperator(
        task_id='create_dataproc',
        cluster_name='test-dataproc-cluster',
        region='europe-west1',
        zone='europe-west1-d',
        project_id='dataops-271513',
        num_workers=2,
        num_masters=1,
        auto_delete_ttl=1200
    )

    print_python_context = PythonOperator(
        task_id='print_the_context',
        provide_context=True,
        python_callable=print_context
    )

    def print_id(ds, **kwargs):
        pprint(kwargs)
        return(ds)

    task_list = Variable.get("task_list", deserialize_json=True)

    for task in task_list:
        generated_task = PythonOperator(
            task_id=f'print_the_context_{task}',
            provide_context=True,
            python_callable=print_id
        )
        print_python_context >> generated_task

    create_dataproc_task
