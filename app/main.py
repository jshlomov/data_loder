from app.db.mongo_database import db_url, db_name, collection_name
from app.servise.attack_service import process_and_insert_data

if __name__ == "__main__":
    first_csv_path = 'db/data/globalterrorismdb_0718dist.csv'
    second_csv_path = 'db/data/RAND_Database_of_Worldwide_Terrorism_Incidents.csv'

    process_and_insert_data(
        first_csv_path,
        second_csv_path,
        db_url,
        db_name,
        collection_name
    )