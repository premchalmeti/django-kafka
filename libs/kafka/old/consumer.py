from confluent_kafka import Consumer

import constants

consumer = None
TOPICS = [constants.LOGIN_TOPIC]


def init_consumer(kafka_config):
    global consumer

    kafka_config.update({
        'group.id': constants.LOGIN_GROUP,
        'auto.offset.reset': 'earliest',
        'client.id': 'login_monitors'
    })

    consumer = Consumer(kafka_config)
    consumer.subscribe(TOPICS)


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def consume():
    print(f'Consumer started listening to events on {", ".join(TOPICS)}...')
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        elif msg.error():
            print(f"Consumer error {msg.format()}")
            continue

        print(f"User logged in ID={msg.key()}, username={msg.value()}")

    consumer.close()


if __name__ == '__main__':
    from utils import setup_django
    setup_django()

    from django.conf import settings
    init_consumer(settings.KAFKA_CONFIG)
    consume()
