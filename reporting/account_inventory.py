"""
This script lists the accounts with nid and category - for account selection on any kind of detail list.
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
SELECT accounts.nid as nid, accounts.name as account, isin, accounts.description as description, 
       categories.name as category, groups.name as bank
FROM accounts
JOIN categories ON categories.nid=accounts.category_id
JOIN groups ON groups.nid=accounts.group_id
ORDER BY groups.name, accounts.name
"""
res = accountdb.get_query(query)
for row in res:
    nid = row['nid']
    account = row['account']
    isin = row['isin']
    description = row['description']
    category = row['category']
    bank = row['bank']
    print(f"{nid}\t{bank}\t{category}\t{account}\t{description}")
logging.info("End Application")
