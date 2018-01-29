import os
 
import boto3
 
from s3select import ResponseHandler
 
CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
MODELS_DIR = os.path.join(CURRENT_DIR, 'models')
os.environ['AWS_DATA_PATH'] = MODELS_DIR
 
 
client = boto3.client('s3', 'us-east-1')
 
 
class PrintingResponseHandler(ResponseHandler):
    def __init__(self, results, *args, **kwargs):
        super(PrintingResponseHandler, self).__init__(*args, **kwargs)
        self.results = results

    def handle_records(self, record_data):
        self.results.append(record_data.decode('utf-8'))
 
 
def handle(event, context):

    bucket = event['bucket']
    key = event['key']
    expression = event['expression']

    if bucket is None or key is None or expression is None:
        raise Exception('bucket, key and expression must all be present')

    response = client.select_object_content(
        Bucket=bucket,
        Key=key,
        SelectRequest={
            'ExpressionType': 'SQL',
            'Expression': expression,
            'InputSerialization': {
                'JSON': {
                    'Type': 'Lines',
                }
            },
            'OutputSerialization': {
                'JSON': {
                    'RecordDelimiter': '\n'
                }
            }
        }
    )

    results = []
    response_handler = PrintingResponseHandler(results)
    response_handler.handle_response(response['Body'])

    return results
