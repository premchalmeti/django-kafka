import io
import json
from abc import ABC

from libs.kafka import constants
from django.conf import settings
import avro
import avro.io
import avro.datafile
import avro.ipc
import avro.schema
from confluent_kafka import Producer


class BaseProducer(ABC):
    TOPIC = None
    SCHEMA = None
    WRITER = None
    kafka = Producer(settings.KAFKA_CONFIG)

    def __init__(self):
        pass

    def serialize_value(self, v):
        return json.dumps(v)

    def serialize_key(self, k):
        return k

    def delivered_callback(self, err, msg):
        if err is not None:
            print(f"Message delivery failed {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def produce(self, data, key=None):
        self.kafka.poll(0)
        self.kafka.produce(
            self.TOPIC,
            key=self.serialize_key(key), 
            value=self.serialize_value(data),
            callback=self.delivered_callback
        )
        self.kafka.flush()


class LoginProducer(BaseProducer):
    TOPIC = constants.LOGIN_TOPIC
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

    def serialize_value(self, v):
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        self.WRITER.write(json.loads(v), encoder)
        encoded_value = bytes_writer.getvalue()
        bytes_writer.flush()
        return encoded_value


class NewBillBoardProducer(BaseProducer):
    TOPIC = constants.LOGIN_TOPIC
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

    def serialize_value(self, v):
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        self.WRITER.write(json.loads(v), encoder)
        encoded_value = bytes_writer.getvalue()
        bytes_writer.flush()
        return encoded_value

