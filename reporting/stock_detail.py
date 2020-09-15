"""
This script collects detail information for a stock.
"""

import argparse
import logging
import os
from lib import my_env
from lib import info_layer


# Initialize Environment
projectname = "gnucash"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")
# Configure command line arguments
parser = argparse.ArgumentParser(
    description="Provide nid for stock account."
)
parser.add_argument('-n', '--nid', type=int, default=165,
                    help='Provide the nid for the stock account')
args = parser.parse_args()
nid = args.nid
logging.info(f"Find detail information for Account {nid}")

accountdb = info_layer.DirectConn(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))

# Get transactions for STOCK or MUTUAL accounts
query = """
select accounts.name as name, isin, date, value_num, value_denom, quantity_num, quantity_denom
from transactions
join accounts on accounts.nid=transactions.account_id
where account_id=165
order by date asc
"""
res = accountdb.get_query(query)
lr = res[-1]
current_price = (lr['value_num']/lr['value_denom']) / (lr['quantity_num']/lr['quantity_denom'])
for row in res:
    name = row['name']
    isin = row['isin']
    date = row['date']
    value_num = row['value_num']
    value_denom = row['value_denom']
    value = value_num/value_denom
    quantity_num = row['quantity_num']
    quantity_denom = row['quantity_denom']
    quantity = quantity_num/quantity_denom
    price = value/quantity
    value_now = current_price*quantity
    delta = value_now - value
    print(f"{name}\t{isin}\t{date}\t{price:.4f}\t{quantity:.4f}\t{value:.4f}\t{value_now:.4f}\t{delta:.4f}")
logging.info("End Application")
