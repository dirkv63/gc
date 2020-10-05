"""
This script collects detail information for an account.
It will collect all transactions for the account.
"""

import argparse
import datetime
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
    description="Provide nid for the account."
)
parser.add_argument('-n', '--nid', type=int, default=165,
                    help='Provide the nid for the stock account')
args = parser.parse_args()
nid = args.nid
logging.info(f"Find detail information for Account {nid}")

pdc = info_layer.PandasConn()

now = datetime.datetime.now().strftime("%Y%m%d")
wbdir = os.getenv('WBDIR')

accounts = pdc.get_accounts()
this_account = accounts[accounts['nid'] == nid].iloc[0]
name = this_account.loc['name'][:31]
wbfile = f"{name}_{now}.xlsx"
wbffn = os.path.join(wbdir, wbfile)


# Configure excel and format
writer = pdc.writer(wbffn)
fmt_dict = info_layer.format_book(writer.book)
# Collect info
df = pdc.get_stock(nid)
df.to_excel(writer, sheet_name=name, index=False)
# Format the output
info_layer.format_stock(writer.sheets[name], fmt_dict)
writer.save()
logging.info("End Application")
