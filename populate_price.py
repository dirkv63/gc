"""
This script populates the price table.
"""

import logging
import os
from lib.db_model import *
from lib import my_env
from lib import info_layer


# Initialize Environment
projectname = "gnucash"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")

accountdb = info_layer.DirectConn(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))
session = info_layer.init_session(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))

# Get transactions for STOCK or MUTUAL accounts
query = """
SELECT shares.account_id as account_id, price, date
FROM shares
JOIN transactions ON transactions.nid=transaction_id
ORDER BY shares.account_id, date desc
"""
res = accountdb.get_query(query)
prev_account_id = -1
for row in res:
    account_id = row['account_id']
    if account_id != prev_account_id:
        prev_account_id = account_id
        price_rec = Price(
            account_id=account_id,
            price=row['price'],
            date=row['date'],
            source='Extract from GnuCash'
        )
        session.add(price_rec)
session.commit()
logging.info("End Application")
