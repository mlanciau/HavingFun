import json
import logging
import argparse

import apache_beam as beam
from apache_beam.runners import DataflowRunner
from apache_beam.options.pipeline_options import PipelineOptions

def ExtractFields(line):
    record = json.loads(line)
    new_record = {}
    new_record['created_at'] = record['created_at'] # core column
    new_record['lang'] = record['lang']
    new_record['id'] = record['id']
    new_record['user_id'] = record['user']['id']
    new_record['user_name'] = record['user']['name']
    new_record['user_screen_name'] = record['user']['screen_name']
    new_record['text'] = record['text']
    new_record['json'] = json.dumps(record) # non core column, will be useful for replaying data pipeline if needed
    return new_record

def run(args, beam_options):
    with beam.Pipeline(options = beam_options) as p:
        jsonTweets = (p | 'ReadMyFile' >> beam.io.ReadFromText(args.input_file))
        parsedJsonTweets = (jsonTweets | 'ExtractFields' >> beam.Map(ExtractFields))
        writeToBigQuery = (parsedJsonTweets | 'writeToBigQuery' >> beam.io.WriteToBigQuery(
            table = 'postgresql.t_tmp_tweets',
            schema = 'created_at:STRING, lang:STRING, id:NUMERIC, user_id:NUMERIC, user_name:STRING, user_screen_name:STRING, text:STRING, json:STRING',
            create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
         )

        p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input-file',
        default='gs://raw_data_dev/data/postgres/*.json',
        help='The file path for the input text to process.'
    )
    args, beam_args = parser.parse_known_args()
    beam_options = PipelineOptions(
        beam_args,
        project = 'dev-project-304310',
        region = 'europe-west1',
        runner = 'DataflowRunner',
        temp_location = 'gs://tmp_data_dev/dataflow',
    )
    run(args, beam_options)
