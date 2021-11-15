import utils
utils.setup_django()

import constants

rec = {"username" : "test message from producer of blip"}

key = '1'

utils.produce(constants.LOGIN_EVENT, key=key, data=rec)
