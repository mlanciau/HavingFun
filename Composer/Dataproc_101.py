#!/usr/bin/env python3

import airflow
from airflow import DAG
from airflow import models
from airflow.utils.dates import days_ago
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator
from datetime import timedelta

default_dag_args = {
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with models.DAG('Dataproc_101',
    schedule_interval=None,
    description='Used for testing security / service account for Dataproc Operator',
    default_args=default_dag_args) as dag:

    t1 = DataprocClusterCreateOperator(
        task_id='create_dataproc',
        cluster_name='test-dataproc-cluster',
        region='europe-west1',
        zone='europe-west1-d',
        project_id='dataops-271513',
        num_workers=1,
        num_masters=1,
        auto_delete_ttl=120
    )

    t1
