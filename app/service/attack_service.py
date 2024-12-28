import pandas as pd
import app.repositories.mongo_repository as mongo_repository
import app.repositories.neo4j_repository as neo4j_repository
import app.repositories.elastic_repository as elastic_repository
import app.repositories.csv_repository as csv_repository
from app.db.mongo_database import attack_collection
import app.service.converting_and_validating_service as conv_and_valid_service


def process_and_insert_data(first_csv_path, second_csv_path):
    first_df = csv_repository.create_df_first_csv(first_csv_path)
    second_df = csv_repository.create_df_second_csv(second_csv_path)
    combined_df = pd.concat([first_df, second_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(
        subset=['date','gname','nkill','nwound','city','country_txt', 'summary'],
        keep='first'
    )
    # attacks = [conv_and_valid_service.validate_and_transform_mongo_models(row) for _, row in combined_df.iterrows()]
    # mongo_repository.insert_many_attacks(attack_collection, attacks)
    #
    # attacks = conv_and_valid_service.validate_and_transform_neo4j_models(combined_df)
    # neo4j_repository.insert_all_data(attacks)

    elastic_data = conv_and_valid_service.validate_and_transform_elastic_models(combined_df)
    elastic_repository.insert_all_data_elastic(elastic_data)



