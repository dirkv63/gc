"""
This script collects detail information for a verzekering having multiple entries.
https://tryolabs.com/blog/2017/03/16/pandas-seaborn-a-guide-to-handle-visualize-data-elegantly/
"""

import argparse
import datetime
import logging
import os
import numpy as np
import pandas as pd
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
                    help='Provide the nid for the verzekering account')
args = parser.parse_args()
nid = args.nid
logging.info(f"Find detail information for Account {nid}")

accountdb = info_layer.DirectConn(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))
cnx = info_layer.connect4pandas()

now = datetime.datetime.now().strftime("%Y%m%d")
wbdir = os.getenv('WBDIR')
wbname = os.getenv('WBNAME')
wbfile = f"{wbname}_{now}.xlsx"
wbffn = os.path.join(wbdir, wbfile)

# Get stock accounts
query = """
SELECT accounts.nid as nid
FROM accounts
JOIN categories ON categories.nid=accounts.category_id
JOIN groups ON groups.nid=accounts.group_id
WHERE categories.name='STOCK' OR categories.name='MUTUAL'
ORDER BY groups.name, accounts.name
"""
accounts = pd.read_sql_query(query, cnx)
nids = [nid for nid in accounts['nid']]
with pd.ExcelWriter(wbffn) as writer:
    for account_id in nids:
        # Get transactions for STOCK or MUTUAL accounts
        print(f"Getting results for account {account_id}")
        query = f"""
        select accounts.name as name, isin, date, value_num, value_denom, quantity_num, quantity_denom
        from transactions
        join accounts on accounts.nid=transactions.account_id
        where account_id={account_id}
        order by date asc
        """
        res = pd.read_sql_query(query, cnx)
        if len(res.index) > 5:
            acc_name = res['name'].iloc[0]
            try:
                current_price = (res['value_num'].iloc[-1]/res['value_denom'].iloc[-1]) / \
                                (res['quantity_num'].iloc[-1]/res['quantity_denom'].iloc[-1])
            except ZeroDivisionError:
                current_price = 0
            res['value'] = np.where(res['value_denom']==0, 0, res['value_num']/res['value_denom'])
            res['quantity'] = np.where(res['quantity_denom']==0, 0, res['quantity_num']/res['quantity_denom'])
            res['value_now'] = res['quantity'] * current_price
            res['delta'] = res['value_now'] - res['value']
            res.drop(['value_num', 'value_denom', 'quantity_num', 'quantity_denom'], axis=1, inplace=True)
            res.to_excel(writer, sheet_name=acc_name[:30], index=False)
logging.info("End Application")
