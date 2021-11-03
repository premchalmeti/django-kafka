from utils import setup_django
setup_django()
from utils import produce

import json
import constants

rec = {"username" : "test message from producer of blip"}

key = '1'
# event = json.dumps(rec)
event = '{"username" : "test message from producer of blip"}'

produce(constants.LOGIN_EVENT, key=key, data=event)
