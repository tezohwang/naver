from pymongo import MongoClient

from .constant import DATABASE

def connect_db(name):
    client = MongoClient(DATABASE[name]['db_uri'])
    db = client[DATABASE[name]['db_name']]
    print("{} - db connected".format(name))
    return db
