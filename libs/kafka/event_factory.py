from typing import Dict, Type
from libs.kafka import constants
from libs.kafka.producers import BaseProducer, LoginProducer


class EventFactory:
    creators: Dict[str, Type[BaseProducer]] = {
        constants.LOGIN_EVENT: LoginProducer
    }

    @classmethod
    def get_producer(cls, eventType: str, *args, **kwargs):
        producer_cls = cls.creators.get(eventType)

        if not producer_cls:
            raise NotImplementedError(f"{eventType} has no implementation")

        return producer_cls(*args, **kwargs)
