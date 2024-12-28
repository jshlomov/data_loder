import os

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv(verbose=True)

client = MongoClient(os.environ['MONGO_DB_URL'])
db = client[os.environ['MONGO_DB_NAME']]
attack_collection = db[os.environ['MONGO_DB_COLLECTION']]
