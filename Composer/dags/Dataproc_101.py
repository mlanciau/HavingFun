import airflow
from airflow import DAG
from airflow import models
from airflow.models import Variable

from airflow.utils.dates import days_ago
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, DataProcHiveOperator, DataProcPySparkOperator

from datetime import timedelta

default_dag_args = {
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with models.DAG('Dataproc_101',
    schedule_interval=None,
    description='Used for testing Dataproc Operator',
    catchup=False,
    max_active_runs=1,
    default_args=default_dag_args) as dag:

    create_dataproc_task = DataprocClusterCreateOperator(
        task_id='create_dataproc',
        cluster_name='test-dataproc-cluster',
        region='europe-west1',
        zone='europe-west1-d',
        project_id='dev-project-304310',
        init_actions_uris=['gs://goog-dataproc-initialization-actions-europe-west1/cloud-sql-proxy/cloud-sql-proxy.sh'],
        metadata={'hive-metastore-instance':'dev-project-304310:europe-west1:pg-hive-metadata'},
        properties={'hive:hive.metastore.warehouse.dir':'gs://raw_data_dev/hive-warehouse'},
        num_workers=2,
        num_masters=1,
        auto_delete_ttl=1200,
    )

    hive_task = DataProcHiveOperator(
        task_id='hive_query',
        cluster_name='test-dataproc-cluster',
        project_id='dev-project-304310',
        query='SHOW TABLES'
    )

#    pyspark_task = DataProcPySparkOperator(
#        task_id='pyspark_job',
#        cluster_name='test-dataproc-cluster',
#        project_id='dev-project-304310',
#        main='gs://europe-west1-composer-dev-445e5e40-bucket/dataproc/',
#    )

    delete_dataproc_task = DataprocClusterDeleteOperator(
        task_id='delete_dataproc',
        cluster_name='test-dataproc-cluster',
        project_id='dev-project-304310',
        region='europe-west1',
    )

    create_dataproc_task >> hive_task >> delete_dataproc_task
