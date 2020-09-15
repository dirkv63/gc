"""
This script populates the shares table.
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
SELECT transactions.nid as nid, account_id, value_num, value_denom, quantity_num, quantity_denom
FROM transactions
JOIN accounts ON accounts.nid=account_id
JOIN categories ON categories.nid=accounts.category_id
WHERE categories.name='STOCK'
   OR categories.name='MUTUAL'
ORDER BY account_id, date asc
"""
res = accountdb.get_query(query)
prev_account_id = -1
tot_shares = 0
cost = 0
li = my_env.LoopInfo('Shares', 100)
for row in res:
    li.info_loop()
    account_id = row['account_id']
    try:
        shares = row['quantity_num'] / row['quantity_denom']
    except ZeroDivisionError:
        shares = 0
    try:
        price = (row['value_num'] / row['value_denom']) / shares
    except ZeroDivisionError:
        price = 0
    if account_id == prev_account_id:
        tot_shares = tot_shares + shares
        cost = cost + price
    else:
        prev_account_id = account_id
        tot_shares = shares
        cost = price
    share_rec = Share(
        account_id=account_id,
        transaction_id=row['nid'],
        price=price,
        shares=tot_shares,
        cost=cost
    )
    session.add(share_rec)
li.end_loop()
session.commit()
logging.info("End Application")
