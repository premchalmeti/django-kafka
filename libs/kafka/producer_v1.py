import json
from confluent_kafka import Producer

import constants

producer = None


def init_producer(kafka_config):
    global producer
    producer = Producer(kafka_config)


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce(key: str, data: object):
    producer.poll(0)
    producer.produce(
        constants.LOGIN_TOPIC, key=key, value=json.dumps(data),
        callback=delivery_report
    )
    producer.flush()


if __name__ == '__main__':
    from utils import setup_django
    setup_django()

    from django.conf import settings
    init_producer(settings.KAFKA_CONFIG)
    import random

    produce(str(int(random.random()*50)), 'premkumar.chalmeti')
