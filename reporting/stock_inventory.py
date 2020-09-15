"""
This script lists the stocks with nid - for stock selection on list_stock details.
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

# Get transactions for STOCK or MUTUAL accounts
query = """
SELECT accounts.nid as nid, accounts.name as account, isin, categories.name as category, groups.name as bank
FROM accounts
JOIN categories ON categories.nid=accounts.category_id
JOIN groups ON groups.nid=accounts.group_id
WHERE categories.name='STOCK' OR categories.name='MUTUAL'"""
res = accountdb.get_query(query)
for row in res:
    nid = row['nid']
    account = row['account']
    isin = row['isin']
    category = row['category']
    bank = row['bank']
    print(f"{bank}\t{nid}\t{category}\t{account}")
logging.info("End Application")
