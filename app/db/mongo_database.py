import os

from dotenv import load_dotenv

load_dotenv(verbose=True)

db_url = os.environ['MONGO_DB_URL']
db_name = "terrorism_db"
collection_name = "attacks"
