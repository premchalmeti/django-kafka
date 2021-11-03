from django.core.management.base import BaseCommand
from libs.kafka.utils import produce
from libs.kafka import constants


class Command(BaseCommand):
    def handle(self, *args, **options):
        import pdb;pdb.set_trace()
        print('producing event to kafka')
        # rec = {"username" : "test message from producer of blip"}

        key = '1'
        # event = json.dumps(rec)
        event = '{"username" : "test message from producer of blip"}'

        produce(constants.LOGIN_EVENT, key=key, data=event)
