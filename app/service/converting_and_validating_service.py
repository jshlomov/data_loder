import os
import uuid
from typing import Optional, List
import pandas as pd
from dotenv import load_dotenv

from app.models.mongo.attack import AttackModel
from app.models.mongo.location import LocationModel
from app.models.neo4j.attack import Attack
from app.models.neo4j.attack_type import AttackType
from app.models.neo4j.group import Group
from app.models.neo4j.location import Location
from app.models.neo4j.target_type import TargetType
load_dotenv(verbose=True)


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
        group_names = [gname for gname in group_names if gname]

        return AttackModel(
            event_id=str(row.get("eventid")),
            date=(row.get("date").to_pydatetime() if isinstance(row.get("date"), pd.Timestamp) else row.get("date"))
            if pd.notna(row.get("date")) else None,
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

def validate_and_transform_neo4j_models(df: pd.DataFrame) -> List[Attack]:
    filtered_df = df.dropna(
        subset=["date", "gname" ,"targtype1_txt", "attacktype1_txt",
                "region_txt", "country_txt", "city", "latitude", "longitude"],
    )
    attacks = [
        attack
        for _, row in filtered_df.iterrows()
        if (
            attack := create_attack(row)
        ) is not None
    ]
    return attacks

def create_attack(row):
    try:
        return Attack(
            date=row["date"],
            group=Group(name=row["gname"]),
            target_type=TargetType(name=row["targtype1_txt"]),
            attack_type=AttackType(name=row["attacktype1_txt"]),
            location=Location(
                region=row["region_txt"],
                country=row["country_txt"],
                city=row["city"],
                latitude=row["latitude"] if not pd.isna(row["latitude"]) else None,
                longitude=row["longitude"] if not pd.isna(row["longitude"]) else None,
            )
        )
    except Exception as e:
        print(f"Error creating Attack for row {row}: {e}")
        return None

def validate_and_transform_elastic_models(df: pd.DataFrame):
    elastic_df = df[["summary", "date"]]
    elastic_df.dropna(inplace=True)
    elastic_dict = elastic_df.to_dict(orient="records")
    elastic_data = [
        {
           '_id': uuid.uuid4(),
           '_index': os.environ['ELASTIC_INDEX_NAME'],
           '_source': row
        }
        for row in elastic_dict
    ]
    return elastic_data