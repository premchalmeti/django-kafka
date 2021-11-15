import io
import json
from abc import ABC, abstractmethod

from django.conf import settings
import avro
import avro.io
import avro.datafile
import avro.ipc
import avro.schema
from confluent_kafka import Producer

from libs.kafka import utils


class BaseProducer(ABC):
    @abstractmethod
    def _serialize_value(self, v):
        pass

    @abstractmethod
    def _serialize_key(self, k):
        pass

    @abstractmethod
    def produce(self):
        pass


class GenericEventProducer(BaseProducer):
    kafka = Producer(settings.KAFKA_CONFIG)

    def __init__(self, event_type: str):
        self.event_type = event_type
        self.topic = utils.get_topic(event_type)

    def _serialize_value(self, v):
        return json.dumps(v)

    def _serialize_key(self, k):
        return k

    def _delivered_callback(self, err, msg):
        if err is not None:
            print(f"Message delivery failed {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def produce(self, data, key=None):
        self.kafka.poll(0)
        self.kafka.produce(
            self.topic,
            key=self._serialize_key(key), 
            value=self._serialize_value(data),
            callback=self._delivered_callback
        )
        self.kafka.flush()


class LoginProducer(GenericEventProducer):
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

    def _serialize_value(self, v):
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        self.WRITER.write(json.loads(v), encoder)
        encoded_value = bytes_writer.getvalue()
        bytes_writer.flush()
        return encoded_value
