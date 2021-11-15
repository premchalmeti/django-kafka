from django.core.management.base import BaseCommand
from libs.kafka import utils, constants


class Command(BaseCommand):
    def handle(self, *args, **options):
        print('producing event to kafka')
        # rec = {"username" : "test message from producer of blip"}

        key = '1'
        # event = json.dumps(rec)
        event = '{"username" : "test message from producer of blip"}'

        utils.produce(constants.LOGIN_EVENT, key=key, data=event)
