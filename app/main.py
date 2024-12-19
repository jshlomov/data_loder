from app.db.mongo_database import db_url, db_name, collection_name
from app.db.repository.attack_repository import AttackRepository
from app.db.repository.csv_repository import load_csv_to_mongo

if __name__ == "__main__":

    file_path = "db/data/terror attacks 1000 rows.csv"

    repository = AttackRepository(db_url, db_name, collection_name)
    result = load_csv_to_mongo(file_path, repository)
    print(f"Inserted {len(result)} records into MongoDB.")