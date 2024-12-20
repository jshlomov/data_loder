from typing import Optional
import pandas as pd
from app.models.mongo.attack import AttackModel
from app.models.mongo.location import LocationModel
from app.repositories.mongo import AttackRepository
from app.repositories.csv.csv_repository import create_df_first_csv, create_df_second_csv


def validate_and_transform_mongo_models(row) -> Optional[AttackModel]:
    try:
        location = LocationModel(
            city=str(row.get("city")) if pd.notna(row.get("city")) else None,
            country=str(row.get("country_txt")) if pd.notna(row.get("country_txt")) else None,
            region=str(row.get("region_txt")) if pd.notna(row.get("region_txt")) else None,
            lat=row.get("latitude") if pd.notna(row.get("latitude")) else None,
            lon=row.get("longitude") if pd.notna(row.get("longitude")) else None,
            location=str(row.get("location")) if pd.notna(row.get("location")) else None
        )

        attack_types = [
            str(row.get("attacktype1_txt")) if pd.notna(row.get("attacktype1_txt")) else None,
            str(row.get("attacktype2_txt")) if pd.notna(row.get("attacktype2_txt")) else None,
            str(row.get("attacktype3_txt")) if pd.notna(row.get("attacktype3_txt")) else None
        ]
        attack_types = [atype for atype in attack_types if atype]

        target_types = [
            str(row.get("targtype1_txt")) if pd.notna(row.get("targtype1_txt")) else None,
            str(row.get("targtype2_txt")) if pd.notna(row.get("targtype2_txt")) else None,
            str(row.get("targtype3_txt")) if pd.notna(row.get("targtype3_txt")) else None
        ]
        target_types = [ttype for ttype in target_types if ttype]

        group_names = [
            str(row.get("gname")) if pd.notna(row.get("gname")) else None,
            str(row.get("gname2")) if pd.notna(row.get("gname2")) else None,
            str(row.get("gname3")) if pd.notna(row.get("gname3")) else None
        ]
        group_names = [gname for gname in group_names if gname]  # Remove None values

        return AttackModel(
            event_id=str(row.get("eventid")),
            date=row.get("date").strftime("%Y-%m-%d") if pd.notna(row.get("date")) else None,
            location=location,
            attack_types=attack_types,
            target_types=target_types,
            group_names=group_names,
            fatalities=int(row.get("nkill")) if pd.notna(row.get("nkill")) else None,
            injuries=int(row.get("nwound")) if pd.notna(row.get("nwound")) else None,
            terrorists_num=int(row.get("nperps")) if pd.notna(row.get("nperps")) else None,
            summary=str(row.get("summary")) if pd.notna(row.get("summary")) else None
        )
    except ValueError as e:
        print(f"Validation error for row {row.get('eventid')}: {e}")
        return None

def process_and_insert_data(first_csv_path, second_csv_path, db_url, db_name, collection_name):
    first_df = create_df_first_csv(first_csv_path)
    second_df = create_df_second_csv(second_csv_path)
    combined_df = pd.concat([first_df, second_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(
        subset=['date','gname','nkill','nwound','city','country_txt', 'summary'],
        keep='first'
    )
    repository = AttackRepository(db_url, db_name, collection_name)

    attacks = [validate_and_transform_mongo_models(row) for _, row in combined_df.iterrows()]
    repository.insert_many_attacks(attacks)