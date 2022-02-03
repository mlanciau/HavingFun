from google.cloud import bigquery
from google.cloud import storage
import base64

def load_to_bigquery(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    table_id = "pso-orange-project.bitcoin.t_bitcoin"
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
    # pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    client = bigquery.Client()
    count = 0
    for blob in storage_client.list_blobs('raw_data_demo', prefix='data/bitcoin'):
        # if count > 10:
        #     break
        uri = f"gs://raw_data_demo/{blob.name}"
        print(f"{uri} start")
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        load_job.result()
        blob.delete()
        print(f"{uri} ok")
        count += 1
