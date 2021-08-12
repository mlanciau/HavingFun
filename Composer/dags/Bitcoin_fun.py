import airflow
from airflow import models
from airflow.models import Variable, TaskInstance
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

import os
import json
import time
from bit import Key
from random import randint
from datetime import timedelta

default_dag_args = {
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

NBR_OF_STEP = 6
NBR_OF_FILE = 10
NBR_OF_LINE = 100000

def generate_key():
    os.makedirs(f"/home/airflow/gcs/data/bitcoin", exist_ok = True)
    rowcount = 0
    translate = {0:'0', 1:'1', 2:'2', 3:'3', 4:'4', 5:'5', 6:'6', 7:'7', 8:'8', 9:'9', 10:'a', 11:'b', 12:'c', 13:'d', 14:'f'}
    for nbr_file in range(NBR_OF_FILE):
        tmp_data = list()
        tmp_data_segwit = list()
        filename = f"data/bitcoin/key_{time.time() * 10000000}"
        for nbr_line in range(NBR_OF_LINE):
            key_tmp = ''
            for i in range(64):
                item = randint(0, 14)
                key_tmp += str(translate[item])
            key = Key.from_hex(key_tmp)
            tmp_data.append((key_tmp, key.address))
            tmp_data_segwit.append((key_tmp, key.segwit_address))
        with open(f"/home/airflow/gcs/{filename}.json", "w") as jsonFile:
            json.dump(tmp_data, jsonFile)
        with open(f"/home/airflow/gcs/{filename}_segwit.json", "w") as jsonFile:
            json.dump(tmp_data_segwit, jsonFile)
    return NBR_OF_FILE * NBR_OF_LINE

with models.DAG('Bitcoin_fun',
    schedule_interval='@hourly',
    description='Demo of DAG relaunch and dynamic task generation',
    catchup=False,
    max_active_runs=1,
    tags=['bitcoin'],
    default_args=default_dag_args) as dag:

    load_file_to_GCS = GCSToGCSOperator(
        task_id='load-file-to-GCS',
        source_bucket = 'europe-west1-composer-dev-445e5e40-bucket',
        source_object = 'data/bitcoin/*.json',
        destination_object = 'data/bitcoin/{{ ds_nodash }}/',
        destination_bucket = 'raw_data_dev',
        move_object = True
    )

    previous_task_1 = None
    previous_task_2 = None

    for step_nbr in range(NBR_OF_STEP):
        generate_bitcoin_key_1 = PythonOperator(
            task_id = f'generate-bitcoin-key-{step_nbr}',
            python_callable = generate_key,
        )

        generate_bitcoin_key_2 = PythonOperator(
            task_id = f'generate-bitcoin-key-{step_nbr + NBR_OF_STEP}',
            python_callable = generate_key,
        )

        if step_nbr == 0:
            previous_task_1 = generate_bitcoin_key_1
            previous_task_2 = generate_bitcoin_key_2
        elif step_nbr == (NBR_OF_STEP - 1):
            previous_task_1 >> generate_bitcoin_key_1 >> load_file_to_GCS
            previous_task_2 >> generate_bitcoin_key_2 >> load_file_to_GCS
        else:
            previous_task_1 >> generate_bitcoin_key_1
            previous_task_2 >> generate_bitcoin_key_2
            previous_task_1 = generate_bitcoin_key_1
            previous_task_2 = generate_bitcoin_key_2

    # trigger_bitcoin_dag = TriggerDagRunOperator(
    #     task_id='trigger_bitcoin_dag',
    #     trigger_dag_id='Bitcoin_fun',
    # )

    # load_file_to_GCS >> trigger_bitcoin_dag
