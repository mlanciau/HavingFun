import airflow
from airflow import models
from airflow.models import Variable, TaskInstance
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago

import json
import time
import tweepy
from datetime import timedelta

default_dag_args = {
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

consumer_key = Variable.get("consumer_key")
consumer_secret = Variable.get("consumer_secret")
access_token = Variable.get("access_token")
access_token_secret = Variable.get("access_token_secret")


def importTweet(key_word, consumer_key, consumer_secret, access_token, access_token_secret):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    public_tweets = api.search(key_word, count=100) # TODO add since_id

    filename = f"/home/airflow/gcs/data/{key_word}/tweet_{time.time() * 1000}.json"

    with open(filename, "w") as jsonFile:
        for tweet in public_tweets:
            json.dump(tweet._json, jsonFile)
            jsonFile.write('\n')
    return filename


with models.DAG('PostgreSQL_tweets',
    schedule_interval='@hourly',
    description='Gather tweets about PostgreSQL',
    catchup=False,
    default_args=default_dag_args) as dag:

    key_word = 'postgres'

    hourly_tweepy_API_call = PythonOperator(
        task_id = 'hourly-tweepy-API-call',
        python_callable = importTweet,
        op_args = [key_word, consumer_key, consumer_secret, access_token, access_token_secret],
    )

    load_file_to_GCS = LocalFilesystemToGCSOperator(
        task_id='load-file-to-GCS',
        src = '{{ ti.xcom_pull(task_ids=\'hourly-tweepy-API-call\') }}',
        dst = key_word + '/{{ ds }}/',
        bucket = 'raw_data_dev'
    )

    hourly_tweepy_API_call >> load_file_to_GCS
