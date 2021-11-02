
def setup_django():
    import os, sys
    import django

    from pathlib import Path
    BASE_DIR = Path(__file__).resolve().parent.parent.parent

    sys.path.append(str(BASE_DIR))
    print(BASE_DIR, 'added')

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'payroll.settings.common')
    django.setup()


def produce(eventType, data, key=None):
    from event_factory import EventFactory
    producer = EventFactory.get_producer(eventType)
    producer.produce(key=key, data=data)
