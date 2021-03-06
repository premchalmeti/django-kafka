import io
import json
from abc import ABC, abstractmethod

import avro
import avro.io
import avro.datafile
import avro.ipc
import avro.schema
from confluent_kafka import Consumer

from libs.kafka import utils


class BaseConsumer(ABC):
    @abstractmethod
    def deserialize_value(self, v):
        pass

    @abstractmethod
    def deserialize_key(self, k):
        pass

    @abstractmethod
    def consume(self):
        pass


class GenericEventConsumer(BaseConsumer):
    kafka = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    })

    def __init__(self, event_type: str):
        self.topic = utils.get_topic(event_type)
        self.kafka.subscribe([self.TOPIC])

    def deserialize_value(self, v):
        return json.loads(v)

    def deserialize_key(self, k):
        return k

    def consume(self):
        while True:
            msg = self.kafka.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            print('Received message: {}'.format(msg.value().decode('utf-8')))

        self.kafka.close()


class LoginConsumer(BaseConsumer):
    SCHEMA = avro.schema.parse(json.dumps({
        "namespace": "users.login",
        "type": "record",
        "name": "Login",
        "fields": [{
            "name": "username",
            "type": ["string", "null"],
            "default": "null"
        }]
    }))
    WRITER = avro.io.DatumWriter(SCHEMA)

    avro.io.DatumWriter(SCHEMA)

    def deserialize_value(self, v):
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        self.WRITER.write(json.loads(v), encoder)
        encoded_value = bytes_writer.getvalue()
        bytes_writer.flush()
        return encoded_value
