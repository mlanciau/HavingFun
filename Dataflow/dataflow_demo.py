import threading
import apache_beam as beam

def addThreadName(text):
    return f'{text},{threading.get_ident()}'

def oneToMany(key, values):
    for value in values:
        yield key, value

def addThreadNameKeyValue(key, value):
    return key, f'{value},{threading.get_ident()}'

def run():
    argv = [
        '--direct_running_mode=multi_threading',
        '--direct_num_workers=4'
    ]
    p = beam.Pipeline(argv=argv)
    (p
        | 'ReadLocalFile' >> beam.io.ReadFromText('/Users/mlanciau/Downloads/tmp Dataflow/input/*.csv', skip_header_lines=True)
        | 'AddThreadName_1' >> beam.Map(addThreadName)
        | 'ParseCSV' >> beam.Map(lambda x:x.split(',', 1))
        | 'GroupByKey' >> beam.GroupByKey()
        | 'FlatMapTuple' >> beam.FlatMapTuple(oneToMany)
        | 'MapTuple' >> beam.MapTuple(addThreadNameKeyValue)
        | 'WriteLocalFile' >> beam.io.WriteToText('/Users/mlanciau/Downloads/tmp Dataflow/output/output')
    )
    p.run().wait_until_finish()

if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--job_date', required=True)
    # parser.add_argument('--twitter_bucket', required=True)
    # parser.add_argument('--dataflow_bucket', required=True)
    # known_args, pipeline_args = parser.parse_known_args()
    # print(known_args)
    # print(pipeline_args)
    run()
