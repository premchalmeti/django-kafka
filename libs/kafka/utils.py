from typing import Union

from logging import getLogger
from django.conf import settings

from libs.kafka import constants


def setup_django():
    import os, sys
    import django

    from pathlib import Path
    BASE_DIR = Path(__file__).resolve().parent.parent.parent

    sys.path.append(str(BASE_DIR))
    print(BASE_DIR, 'added')

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'payroll.settings.common')
    django.setup()


def get_topic(event_type: str) -> str:
    return constants.EVENT_TOPIC_MAPPING.get(event_type)


def produce(event_type: str, data: Union[list, dict], key=None):
    from libs.kafka.factory import ProducerFactory
    try:
        producer = ProducerFactory.get_producer(event_type)
        producer.produce(key=key, data=data)
    except Exception:
        getLogger(__name__).exception(
            f"Kafka produce failed for {event_type}", exc_info=True
        )


def consume(event_type: str):
    from libs.kafka.factory import ConsumerFactory
    try:
        consumer = ConsumerFactory.get_consumer(event_type)
        consumer.consume()
    except Exception:
        getLogger(__name__).exception(
            f"Kafka produce failed for {event_type}", exc_info=True
        )


def raise_if_kafka_disabled():
    if not settings.KAFKA_ENABLED:
        raise EnvironmentError("KAFKA_RUN is disabled")
