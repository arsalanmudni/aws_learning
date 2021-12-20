from confluent_kafka import Consumer, KafkaException, KafkaError
from avro.schema import parse
from avro.io import DatumReader, BinaryDecoder
import io
import sys

if __name__ == "__main__":

    conf = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'my_group',
            'default.topic.config': {'auto.offset.reset': 'smallest'}}
    kafka_consumer = Consumer(**conf)
    topic = kafka_consumer.subscribe(['test-topic'])

    schema_path = "student.avsc"
    schema = parse(open(schema_path).read())

    try:
        running = True
        while running:
            res = kafka_consumer.poll(timeout=30000)
            if res is None:
                continue
            if res.error():
                if res.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('{} {} reached end at offset {}}\n'.format(res.topic(), res.partition(), res.offset()))
                elif res.error():
                    raise KafkaException(res.error())
            else:
                sys.stderr.write('{} {} at offset {} with key {}:\n'.format(res.topic(), res.partition(), res.offset(),
                                  str(res.key())))

            message = res.value()
            bytes_reader = io.BytesIO(message)
            decoder = BinaryDecoder(bytes_reader)
            reader = DatumReader(schema)
            try:
                decoded_msg = reader.read(decoder)
                print(decoded_msg)
                sys.stdout.flush()
            except AssertionError:
                continue

    except Exception:
        sys.stderr.write('Terminated\n')