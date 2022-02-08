from google.cloud import bigquery
from google.cloud import storage
from datetime import date
import base64

def load_to_bigquery(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("c_key", "STRING"),
            bigquery.SchemaField("c_address", "STRING"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('raw_data_demo')
    # pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    client = bigquery.Client()
    today = date.today()
    count = 0
    iterator = bucket.list_blobs(delimiter='/', prefix='data/bitcoin/')
    list(iterator)
    for i in iterator.prefixes:
        if not today.strftime("%Y%m%d") in i:
            table_id = f"pso-orange-project.bitcoin.t_bitcoin_{i.split('/')[2]}"
            tmp_iterator = bucket.list_blobs(delimiter='/', prefix=i)
            list(tmp_iterator)
            for folder in tmp_iterator.prefixes:
                count += 1
                uri = f'gs://raw_data_demo/{folder}*.csv'
                print(f"{uri} start")
                load_job = client.load_table_from_uri(
                    uri, table_id, job_config=job_config
                )
                load_job.result()
                print(f"{uri} load ok")
                bucket.delete_blobs(blobs=list(bucket.list_blobs(prefix=folder)))
                print(f"{folder} delete ok")
                if count > 2:
                    return 0
