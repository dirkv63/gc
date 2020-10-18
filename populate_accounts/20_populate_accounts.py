"""
This script collects the list of all accounts.
"""

import logging
import os
from lib.db_model import *
from lib import my_env
from lib import info_layer


def handle_account(acc_row, bank_id, p_id=None):
    """
    This function gets an account row, adds it to the accounts table then finds all children for this account in a 
    recursive way. This allows to transfer the parent_id to the children records.
    
    :param acc_row: Result of query to accounts table
    :param bank_id: ID of the Bank or Group for the account.
    :param p_id: Immediate Parent of the account
    :return: 
    """
    acc_guid = acc_row['guid']
    account = Account(
        name=acc_row['name'],
        guid=acc_guid,
        category_id=cats[acc_row['account_type']],
        group_id=bank_id,
        placeholder=acc_row['placeholder'],
        description=acc_row['code'],
        commodity_guid=acc_row['commodity_guid']
    )
    if p_id:
        account.parent_id = p_id
    if acc_row['account_type'] == 'STOCK' or acc_row['account_type'] == 'MUTUAL':
        account.isin = acc_row['cusip']
    session.add(account)
    session.commit()
    session.refresh(account)
    parent_id = account.nid
    acc_query = f"""
                SELECT accounts.guid as guid, name, account_type, cusip, placeholder, code, commodity_guid
                FROM accounts 
                LEFT JOIN commodities on commodities.guid=commodity_guid
                WHERE parent_guid='{acc_guid}' 
                AND hidden=0
                """
    acc_res = gnudb.get_query(acc_query)
    for acc_row in acc_res:
        handle_account(acc_row, bank_id, parent_id)


# Initialize Environment
projectname = "gnucash"
config = my_env.init_env(projectname, __file__)
logging.info("Start application")

gnudb = info_layer.DirectConn(os.getenv('GNUDIR'), os.getenv('GNUNAME'))
session = info_layer.init_session(os.getenv('ACCOUNTDIR'), os.getenv('ACCOUNTNAME'))
cats = {}
groups = {}

# First populate categories table and remember nid in cats dictionary.
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
# Start with Root account - this is the initial parent but will not be stored
root = 'Root Account'
query = f"SELECT guid FROM accounts where name='{root}'"
res = gnudb.get_query(query)
row = res[0]
parent_guid = row['guid']
# Direct children of the Root account are Bank identifiers or Group (in - uit - ...) identifiers.
# These are to be stored in the groups table.
query = f"""
SELECT guid, name, code, account_type as category 
FROM accounts 
WHERE parent_guid='{parent_guid}' AND hidden=0
"""
res = gnudb.get_query(query)
for row in res:
    group = Group(
        name=row['name'],
        guid=row['guid'],
        description=row['code'],
        category=row['category']
    )
    session.add(group)
    session.commit()
    session.refresh(group)
    groups[row['guid']] = group.nid

# Next find all accounts related to a Bank or Group.
# Find name, guid, category for every account - recursively from root account.
# Bank and Group accounts are identified with 'Placeholder=1'. A Bank account can have a Sub-Group (Effectenrekening).
query = f"""
SELECT accounts.guid as guid, name, account_type, cusip, placeholder, code, commodity_guid
FROM accounts 
LEFT JOIN commodities on commodities.guid=commodity_guid
WHERE parent_guid='{parent_guid}' 
AND hidden=0
"""
res = gnudb.get_query(query)
for row in res:
    group_id = groups[row['guid']]
    handle_account(row, group_id)
