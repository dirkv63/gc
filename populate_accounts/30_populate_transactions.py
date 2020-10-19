"""
This script collects the transactions.
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

gnudb = info_layer.DirectConn(os.getenv('GNUDIR'), os.getenv('GNUNAME'))
accountdb = info_layer.DirectConn(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))
session = info_layer.init_session(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))
accounts = {}

# Get nids for the accounts
query = "SELECT nid, guid FROM accounts"
res = accountdb.get_query(query)
for row in res:
    accounts[row['guid']] = row['nid']

# Get transactions
query = """
SELECT account_guid, post_date, action, value_num, value_denom, quantity_num, quantity_denom, 
       transactions.description as description
FROM splits
LEFT JOIN transactions ON transactions.guid=tx_guid
"""
res = gnudb.get_query(query)
li = my_env.LoopInfo('Transactions', 1000)
for row in res:
    li.info_loop()
    account = row['account_guid']
    try:
        account_id = accounts[account]
    except KeyError:
        # Account not found, must be from a hidden / closed account
        pass
    else:
        tx = Transaction(
            account_id=account_id,
            date=row['post_date'][:10],
            action=row['action'],
            value_num=row['value_num'],
            value_denom=row['value_denom'],
            quantity_num=row['quantity_num'],
            quantity_denom=row['quantity_denom'],
            description=row['description']
        )
        session.add(tx)
li.end_loop()
session.commit()
logging.info("End Application")
