from django.core.management.base import BaseCommand
from libs.kafka.utils import consume
from libs.kafka import constants


class Command(BaseCommand):
    def handle(self, *args, **options):
        print(f'consuming events on {constants.LOGIN_EVENT} kafka topic')
        consume(constants.LOGIN_EVENT)
