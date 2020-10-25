"""
This is the data model.
"""

from sqlalchemy import Column, Integer, Text, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Account(Base):
    """
    Table containing account information.
    """
    __tablename__ = "accounts"
    nid = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text, nullable=False)
    guid = Column(Text, nullable=False, unique=True)
    isin = Column(Text)
    currency = Column(Text)
    placeholder = Column(Integer)
    description = Column(Text)
    commodity_guid = Column(Text)
    category_id = Column(Integer, ForeignKey('categories.nid'), comment="ID of the category")
    group_id = Column(Integer, ForeignKey('groups.nid'), comment="ID of the group")
    parent_id = Column(Integer, ForeignKey('accounts.nid'))
    group = relationship("Group", backref="hasAccounts")

class Category(Base):
    """
    Table containing categories
    """
    __tablename__ = "categories"
    nid = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text, nullable=False)
    cat_type = Column(Text, nullable=False)

class Group(Base):
    """
    Table containing the group information. This is Top-level information, such as Bank, In, Pension, ...
    For Bank accounts - taken into account for Overview - the of the bank is on the Code field.
    """
    __tablename__ = "groups"
    nid = Column(Integer, primary_key=True, autoincrement=True)
    guid = Column(Text, nullable=False, unique=True)
    name = Column(Text, nullable=False)
    description = Column(Text)
    category = Column(Text)

class Transaction(Base):
    """
    Table containing transaction records.
    """
    __tablename__ = "transactions"
    nid = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('accounts.nid'), nullable=False)
    date = Column(Text, nullable=False)
    action = Column(Text)
    value_num = Column(Integer)
    value_denom = Column(Integer)
    quantity_num = Column(Integer)
    quantity_denom = Column(Integer)
    description = Column(Text)

class Share(Base):
    """
    Table containing the shares records
    """
    __tablename__ = "shares"
    nid = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('accounts.nid'), nullable=False)
    transaction_id = Column(Integer, ForeignKey('transactions.nid'), nullable=False)
    price = Column(Float)
    shares = Column(Float)
    cost = Column(Float)

class Price(Base):
    """
    Table containing Price information for commodities.
    """
    __tablename__ = "price"
    nid = Column(Integer, primary_key=True, autoincrement=True)
    commodity_guid = Column(Text, nullable=False)
    date = Column(Text)
    currency = Column(Text, nullable=False)
    value_num = Column(Integer)
    value_denom = Column(Integer)

class XRate(Base):
    """
    Table containing Exchange Rate information.
    """
    __tablename__ = "xrate"
    nid = Column(Integer, primary_key=True, autoincrement=True)
    local_curr = Column(Text, nullable=False)
    foreign_curr = Column(Text, nullable=False)
    date = Column(Text)
    value_num = Column(Integer)
    value_denom = Column(Integer)
