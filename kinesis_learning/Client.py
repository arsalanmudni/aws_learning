import boto3
import json

class KinesisClient:

    def __init__(self, stream_name):
        self.client = boto3.client('kinesis')
        self.stream_name = stream_name

    def desc_stream(self):
        stream_desc = self.client.describe_stream(
            StreamName=self.stream_name
        )
        return stream_desc

    def create_data_stream(self, shard_count):
        self.client.create_stream(
            StreamName=self.stream_name,
            ShardCount=shard_count
        )

    def delete_data_stream(self):
        self.client.delete_stream(
            StreamName='test_stream1',
            EnforceConsumerDeletion=True
        )

    def save_data(self, data, partition_key):
        kinesis_records = [{'Data': json.dumps(record),
                            'PartitionKey': record[partition_key]}
                           for record in data]
        self.client.put_records(Records=kinesis_records, StreamName=self.stream_name)

    def get_data_stream(self):
        response = self.client.describe_stream(StreamName=self.stream_name)
        data = []
        stream_desc = response['StreamDescription']
        if stream_desc['StreamStatus'] == 'ACTIVE':
            for shard_id_data in stream_desc['Shards']:
                shard_id = shard_id_data['ShardId']
                starting_sequence_number = shard_id_data['SequenceNumberRange']['StartingSequenceNumber']
                resp = self.client.get_shard_iterator(
                    StreamName='test_stream1',
                    ShardId=shard_id,
                    ShardIteratorType='AT_SEQUENCE_NUMBER',
                    StartingSequenceNumber=starting_sequence_number
                )
                shard_iterator = resp.get('ShardIterator')
                if shard_iterator:
                    rec = self.client.get_records(
                        ShardIterator=shard_iterator
                    )
                    print(rec)
                    shard_data = rec.get('Records')
                    if not shard_data:
                        break
                    data.extend(shard_data)
        return data