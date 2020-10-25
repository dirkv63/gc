"""
This script populates the accounts database and creates the consolidation excel.
"""

# Allow lib to library import path.
import os
import logging
from lib import my_env
from lib.my_env import run_script

scripts = [
    "10_rebuild_db",
    "20_populate_accounts",
    "30_populate_transactions",
    "40_populate_price",
    "50_update_account_currency",
    "consolidation"
    ]

cfg = my_env.init_env("gnu", __file__)
logging.info("Start Application")
(fp, filename) = os.path.split(__file__)
for script in scripts:
    logging.info("Run script: {s}.py".format(s=script))
    run_script(fp, "{s}.py".format(s=script))
logging.info("End Application")