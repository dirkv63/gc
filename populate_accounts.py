"""
This script collects the list of all occounts.
"""

import logging
import os
from lib.db_model import *
from lib import my_env
from lib import info_layer


def handle_account(acc_row, g_id, p_id=None):
    # global cats
    acc_guid = acc_row['guid']
    account = Account(
        name=acc_row['name'],
        guid=acc_guid,
        category_id=cats[acc_row['account_type']],
        group_id=g_id
    )
    if p_id:
        account.parent_id = p_id
    session.add(account)
    session.commit()
    session.refresh(account)
    parent_id = account.nid
    acc_query = f"SELECT guid, name, account_type FROM accounts WHERE parent_guid='{acc_guid}' AND hidden=0"
    acc_res = gnudb.get_query(acc_query)
    for acc_row in acc_res:
        handle_account(acc_row, g_id, parent_id)


# Initialize Environment
projectname = "gnucash"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")

gnudb = info_layer.DirectConn(os.getenv('GNUDIR'), os.getenv('GNUNAME'))
session = info_layer.init_session(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))
cats = {}
groups = {}

# First populate categories
query = "SELECT DISTINCT account_type FROM accounts"
res = gnudb.get_query(query)
for row in res:
    cat = row['account_type']
    cat_row = Category(
        name=cat
    )
    session.add(cat_row)
    session.commit()
    session.refresh(cat_row)
    cats[cat] = cat_row.nid

# Then get Group / Bank Accounts
root = 'Root Account'
query = f"SELECT guid FROM accounts where name='{root}'"
res = gnudb.get_query(query)
row = res[0]
parent_guid = row['guid']
query = f"SELECT guid, name FROM accounts WHERE parent_guid='{parent_guid}' AND hidden=0"
res = gnudb.get_query(query)
for row in res:
    group = Group(
        name=row['name'],
        guid=row['guid']
    )
    session.add(group)
    session.commit()
    session.refresh(group)
    groups[row['guid']] = group.nid

# Find name, guid, category for every account - recursively from root account
query = f"SELECT guid, name, account_type FROM accounts WHERE parent_guid='{parent_guid}' AND hidden=0"
res = gnudb.get_query(query)
for row in res:
    group_id = groups[row['guid']]
    handle_account(row, group_id)
