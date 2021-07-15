import airflow
from airflow import DAG
from airflow import models
from airflow.models import Variable
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago

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

def importTweet(consumer_key, consumer_secret, access_token, access_token_secret):
    import tweepy

    key_word = 'postgres'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    public_tweets = api.search(key_word, count=10) # TODO add since_id

    data = list()

    for tweet in public_tweets:
        data.append(tweet)
    return data


with models.DAG('PostgreSQL_tweets',
    schedule_interval='@hourly',
    description='Gather tweets about PostgreSQL',
    catchup=False,
    default_args=default_dag_args) as dag:

    hourly_tweepy_API_call = PythonVirtualenvOperator(
        task_id='hourly-tweepy-API-call',
        python_callable=importTweet,
        op_args=[consumer_key, consumer_secret, access_token, access_token_secret],
        requirements=["tweepy"],
        system_site_packages=False
    )

    hourly_tweepy_API_call
