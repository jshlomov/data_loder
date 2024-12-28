import pandas as pd
from app.repositories.csv_repository import create_df_first_csv, create_df_second_csv
from app.repositories.neo4j_repository import insert_all_data
from app.service.converting_and_validating_service import validate_and_transform_neo4j_models


def process_and_insert_data(first_csv_path, second_csv_path):
    first_df = create_df_first_csv(first_csv_path)
    second_df = create_df_second_csv(second_csv_path)
    combined_df = pd.concat([first_df, second_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(
        subset=['date','gname','nkill','nwound','city','country_txt', 'summary'],
        keep='first'
    )
    # attacks = [validate_and_transform_mongo_models(row) for _, row in combined_df.iterrows()]
    # mongo_repository.insert_many_attacks(attack_collection, attacks)

    attacks = validate_and_transform_neo4j_models(combined_df)
    insert_all_data(attacks)



