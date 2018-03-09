from pymongo import MongoClient

from .constant import DATABASE

def connect_db(name):
    client = MongoClient(DATABASE[name]['db_uri'])
    db = client[DATABASE[name]['db_name']]
    print("{} - db connected".format(name))
    return db

def save_accounts(accounts):
    pass

def save_campaigns(campaigns):
    pass

def save_adgroups(adgroups):
    pass

def save_insights(insights):
    pass
