"""
This procedure will rebuild the database
"""

import logging
import os
from lib import my_env
from lib import info_layer

my_env.init_env("gnucash", __file__)
logging.info("Start application")
sm = info_layer.DirectConn(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))
sm.rebuild()
logging.info("Database rebuild")
