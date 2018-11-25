from pymongo import MongoClient

def connect(db, collection):
    client = MongoClient('localhost', 27017)
    db = client[db]
    collection = db[collection]
    return collection