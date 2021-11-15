from typing import Type

from libs.kafka import constants, utils
from libs.kafka.producers import BaseProducer, LoginProducer
from libs.kafka.consumers import BaseConsumer, LoginConsumer


class ProducerFactory:
    creators: dict[str, Type[BaseProducer]] = {
        constants.LOGIN_EVENT: LoginProducer
    }

    @classmethod
    def get_producer(cls, event_type: str, *args, **kwargs):
        utils.raise_if_kafka_disabled()
        producer_cls = cls.creators.get(event_type)

        if not producer_cls:
            raise NotImplementedError(f"{event_type} has no implementation")

        return producer_cls(event_type=event_type, *args, **kwargs)

    @classmethod
    def register_produce(cls, event_type: str, producer_cls: Type[BaseProducer]):
        cls.creators[event_type] = producer_cls

    @classmethod
    def get_cls(cls, event_type: str) -> Type[BaseProducer]:
        return cls.creators.get(event_type)


class ConsumerFactory:
    creators: dict[str, Type[BaseConsumer]] = {
        constants.LOGIN_EVENT: LoginConsumer
    }

    @classmethod
    def get_consumer(cls, event_type: str, *args, **kwargs):
        utils.raise_if_kafka_disabled()
        consumer_cls = cls.creators.get(event_type)

        if not consumer_cls:
            raise NotImplementedError(f"{event_type} has no implementation")

        return consumer_cls(event_type=event_type, *args, **kwargs)

    @classmethod
    def register_consumer(cls, event_type: str, consumer_cls: Type[BaseConsumer]):
        cls.creators[event_type] = consumer_cls

    @classmethod
    def get_cls(cls, event_type: str) -> Type[BaseConsumer]:
        return cls.creators.get(event_type)
