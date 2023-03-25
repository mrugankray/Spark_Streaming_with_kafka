import requests
import json

# 3rd party library imported
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import Schema

# imort from constants
from constants import SCHEMA_STR, ANALYZED_DATA_SCHEMA

init_string = 'data: '
source_url = 'https://stream.wikimedia.org/v2/stream/recentchange'
kafka_url = 'localhost:9092'
schema_registry_url = 'http://localhost:8083'
kafka_topic = 'wikimedia'
kafka_analyzed_data_topic = 'wikimedia.processed'
schema_registry_subject = f"{kafka_topic}-value"
schema_registry_analyzed_data_subject = f"{kafka_analyzed_data_topic}-value"

def delivery_report(errmsg, msg):
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(msg.key(), msg.topic(), msg.partition(), msg.offset()))

def avro_producer(source_url, kafka_url, schema_registry_url, schema_registry_subject):
    # schema registry
    sr, latest_version = get_schema_from_schema_registry(schema_registry_url, schema_registry_subject)


    value_avro_serializer = AvroSerializer(schema_registry_client = sr,
                                          schema_str = latest_version.schema.schema_str,
                                          conf={
                                              'auto.register.schemas': False
                                            }
                                          )

    # Kafka Producer
    producer = SerializingProducer({
        'bootstrap.servers': kafka_url,
        'security.protocol': 'plaintext',
        'value.serializer': value_avro_serializer,
        'delivery.timeout.ms': 120000, # set it to 2 mins
        'enable.idempotence': 'true'
    })

    s = requests.Session()

    with s.get(source_url, headers=None, stream=True) as resp:
        for line in resp.iter_lines():
            if line:
                decoded_line = line.decode()
                if decoded_line.find(init_string) >= 0:
                    # remove data: to create a valid json
                    decoded_line = decoded_line.replace(init_string, "")
                    # convert to json
                    decoded_json = json.loads(decoded_line)
                    
                    try:
                        # print(decoded_line + '\n')
                        producer.produce(topic=kafka_topic, value=decoded_json, on_delivery=delivery_report)

                        # Trigger any available delivery report callbacks from previous produce() calls
                        events_processed = producer.poll(1)
                        print(f"events_processed: {events_processed}")

                        messages_in_queue = producer.flush(1)
                        print(f"messages_in_queue: {messages_in_queue}")
                    except Exception as e:
                        print(e)

def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)
    # print(f"schema_id: {latest_version.schema_id}")
    # print(f"schema_str: {latest_version.schema.schema_str}")

    return sr, latest_version

def register_schema(schema_registry_url, schema_registry_subject, schema_str):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    schema = Schema(schema_str, schema_type="AVRO")
    schema_id = sr.register_schema(subject_name=schema_registry_subject, schema=schema)

    return schema_id

def update_schema(schema_registry_url, schema_registry_subject, schema_str):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    versions_deleted_list = sr.delete_subject(schema_registry_subject)
    print(f"versions of schema deleted list: {versions_deleted_list}")

    schema_id = register_schema(schema_registry_url, schema_registry_subject, schema_str)
    return schema_id

# register_schema(schema_registry_url, schema_registry_analyzed_data_subject, ANALYZED_DATA_SCHEMA)

# avro_producer(source_url, kafka_url, schema_registry_url, schema_registry_subject)

# get_schema_from_schema_registry(schema_registry_url, schema_registry_subject)

# update_schema(schema_registry_url, schema_registry_analyzed_data_subject, ANALYZED_DATA_SCHEMA)
