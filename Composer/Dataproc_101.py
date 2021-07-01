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

with models.DAG('CI_CD_test',
    schedule_interval=None,
    description='Used for setting up the CI CD workflow',
    default_args=default_dag_args) as dag:

    t1 = DataprocClusterCreateOperator(
        task_id='create_dataproc',
        cluster_name='test',
        num_workers=1,
        num_masters=1,
        auto_delete_ttl=120
    )

    t1
