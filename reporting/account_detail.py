"""
This script collects detail information for an account.
It will collect all transactions for the account.
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
    description="Provide nid for the account."
)
parser.add_argument('-n', '--nid', type=int, default=17,
                    help='Provide the nid for the account')
args = parser.parse_args()
nid = args.nid
logging.info(f"Find detail information for Account {nid}")

accountdb = info_layer.DirectConn(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))
cnx = info_layer.connect4pandas()

now = datetime.datetime.now().strftime("%Y%m%d")
wbdir = os.getenv('WBDIR')
wbname = os.getenv('WBNAME')
wbfile = f"{wbname}_{nid}_{now}.xlsx"
wbffn = os.path.join(wbdir, wbfile)

query = f"""
SELECT accounts.name as name, isin, date, transactions.description, 
       value_num, value_denom, quantity_num, quantity_denom
FROM transactions
JOIN accounts ON accounts.nid=transactions.account_id
WHERE account_id={nid}
ORDER BY date asc
"""
with pd.ExcelWriter(wbffn) as writer:
    res = pd.read_sql_query(query, cnx)
    res['value'] = np.where(res['value_denom'] == 0, 0, res['value_num'] / res['value_denom'])
    res['quantity'] = np.where(res['quantity_denom'] == 0, 0, res['quantity_num'] / res['quantity_denom'])
    res.drop(['value_num', 'value_denom', 'quantity_num', 'quantity_denom'], axis=1, inplace=True)
    res.to_excel(writer, index=False)
logging.info("End Application")
