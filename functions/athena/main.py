import os
import datetime
 
import boto3

ATHENA_DB = 'fetches'
ATHENA_TABLE = 'fetches_realtime_gzipped'
# test query: 'SELECT * FROM fetches_realtime_gzipped LIMIT 1'
# todo: use below boto3.client('sts').get_caller_identity()['Account']
ATHENA_RESULT_BUCKET = 's3://aws-athena-query-results-470049585876-us-east-1'

client = boto3.client('athena', 'us-east-1')

def handle(event, context):
    query = event['query']
    today = datetime.datetime.today()
    output_location = f'{ATHENA_RESULT_BUCKET}/{today.year}/{today.month}/{today.day}'

    if query is None:
        raise Exception('query must be present')

    query_id = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': ATHENA_DB
        },
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )

    return query_id
