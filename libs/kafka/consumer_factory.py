from typing import Dict, Type
from libs.kafka import constants
from libs.kafka.consumers import BaseConsumer, LoginConsumer


class ConsumerFactory:
    creators: Dict[str, Type[BaseConsumer]] = {
        constants.LOGIN_EVENT: LoginConsumer
    }

    @classmethod
    def get_consumer(cls, eventType: str, *args, **kwargs):
        consumer_cls = cls.creators.get(eventType)

        if not consumer_cls:
            raise NotImplementedError(f"{eventType} has no implementation")

        return consumer_cls(*args, **kwargs)
