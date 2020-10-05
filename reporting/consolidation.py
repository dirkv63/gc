"""
This script creates the consolidation report containing a workbook with an overview sheet and detail sheets for all
stocks and verzekeringen.
"""
import datetime
import logging
import os
from lib import my_env
from lib import info_layer


# Initialize Environment
projectname = "gnucash"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")
pdc = info_layer.PandasConn()

# Prepare Excel Workbook
now = datetime.datetime.now().strftime("%Y%m%d")
wbdir = os.getenv('WBDIR')
wbname = os.getenv('WBNAME')
wbfile = f"{wbname}_{now}.xlsx"
wbffn = os.path.join(wbdir, wbfile)
writer = pdc.writer(wbffn)
fmt_dict = info_layer.format_book(writer.book)

# Get overview sheet first
name = "Overview"
df = pdc.get_account_summary()
df.to_excel(writer, sheet_name=name, index=False)
# Format the output
info_layer.format_summary(writer.sheets[name], fmt_dict)

# Then get accounts sheets
accounts = pdc.get_all_savings()
for index, row in accounts.iterrows():
    # Get transactions for STOCK or MUTUAL accounts
    account = row['name'][:30]
    print(f"Getting results for account {account}")
    if row['category'] == "BANK":
        df = pdc.get_verzekering(row['nid'])
        if len(df.index) > 1:
            df.to_excel(writer, sheet_name=account, index=False)
            # Format the output
            info_layer.format_verzekering(writer.sheets[account], fmt_dict)
    else:
        df = pdc.get_stock(row['nid'])
        if len(df.index) > 1:
            df.to_excel(writer, sheet_name=account, index=False)
            # Format the output
            info_layer.format_stock(writer.sheets[account], fmt_dict)
writer.save()
logging.info("End Application")
