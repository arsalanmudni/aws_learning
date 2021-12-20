import json

from kinesis_learning.Client import KinesisClient


def lambda_handler(event, context):
    print("Start \n")
    kinesis_client = KinesisClient('test_stream1')
    data = kinesis_client.get_data_stream()
    print("Data : {}".format(data))

    return {
        'statusCode': 200,
        'body': json.dumps(data)
    }
