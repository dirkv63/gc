"""
This script collects the list of all accounts.
"""

import logging
import os
from lib import my_env
from lib import info_layer

# Initialize Environment
projectname = "gnucash"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")

accountdb = info_layer.DirectConn(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))
query = """
UPDATE accounts
SET currency=
    (SELECT price.currency
    FROM price
    WHERE accounts.commodity_guid=price.commodity_guid)
WHERE accounts.currency IS NULL
"""
accountdb.run_query(query)
accountdb.close()
