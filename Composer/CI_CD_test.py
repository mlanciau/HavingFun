#!/usr/bin/env python3

import airflow
from airflow import DAG
from airflow import models
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

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

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    t1
