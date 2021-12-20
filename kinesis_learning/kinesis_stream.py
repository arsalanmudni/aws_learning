import time

from kinesis_learning.Client import KinesisClient


def lambda_handler(event, context):
    kinesis_client = KinesisClient('test_stream1')
    response = kinesis_client.create_data_stream(12)
    print("Stream response {}".format(response))

    stream_status = kinesis_client.desc_stream()
    while stream_status['StreamDescription']['StreamStatus'] == 'CREATING':
        print(stream_status['StreamDescription']['StreamStatus'])
        time.sleep(1)
        stream_status = kinesis_client.desc_stream()
    # print("Stream status {}".format(stream_status))
    data = [
        {"id": "5001", "type": "None"},
        {"id": "5002", "type": "Glazed"},
        {"id": "5005", "type": "Sugar"},
        {"id": "5007", "type": "Powdered Sugar"},
        {"id": "5006", "type": "Chocolate with Sprinkles"},
        {"id": "5003", "type": "Chocolate"},
        {"id": "5004", "type": "Maple"}
    ]

    kinesis_client.save_data(data, 'id')

    response = kinesis_client.delete_data_stream()
    print("Delete Response {}".format(response))
