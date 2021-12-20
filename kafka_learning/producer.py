from confluent_kafka import Producer
from avro.schema import parse
from avro.io import DatumWriter, BinaryEncoder
import io


if __name__ == "__main__":

    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(**conf)
    topic = "test-topic"

    # Path to student.avsc avro schema
    schema_path = "student.avsc"
    schema = parse(open(schema_path).read())

    for i in range(10):
        writer = DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer.write({"name": "123",
                      "Class": "Intermediate",
                      "RollNo": i+1}, encoder)
        raw_bytes = bytes_writer.getvalue()
        producer.produce(topic, raw_bytes)
    producer.flush()