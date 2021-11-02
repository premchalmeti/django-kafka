import json
from abc import ABC

import constants
from django.conf import settings
from confluent_kafka import Producer


class BaseProducer(ABC):
    TOPIC = None
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
    SCHEMA = {}
    WRITER = object()
