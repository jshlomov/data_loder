import math
from dataclasses import asdict, dataclass
from typing import List
import pandas as pd
from tqdm import tqdm
from app.db.neo4j_database import driver
from app.models.neo4j.neo4j_models import Attack, Group, TargetType, AttackType, Location


def prepare_data_from_df(df: pd.DataFrame) -> List[Attack]:
    filtered_df = df.dropna(
        subset=["date", "gname","targtype1_txt", "attacktype1_txt",
                "region_txt", "country_txt", "city", "latitude", "longitude"],
    )
    attacks = []
    for _, row in filtered_df.iterrows():
        try:
            attack = Attack(
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
            attacks.append(attack)
        except Exception as e:
            print(f"Error preparing data for row {row}: {e}")
    return attacks


def insert_all_data(attacks: List[Attack]):
    query = """
        UNWIND $rows AS row
        MERGE (g:Group {name: row.group.name})
        MERGE (t:TargetType {name: row.target_type.name})
        MERGE (at:AttackType {name: row.attack_type.name})
        MERGE (l:Location {
            region: row.location.region,
            country: row.location.country
        })
        ON CREATE SET 
            l.latitude = row.location.latitude,
            l.longitude = row.location.longitude
        CREATE (a:Attack {date: row.date})
        CREATE (g)-[:CONDUCTED]->(a)
        CREATE (a)-[:TARGETED]->(t)
        CREATE (a)-[:USED]->(at)
        CREATE (a)-[:OCCURRED_AT]->(l)
    """

    rows = [asdict(attack) for attack in attacks]
    batch_size = 2000
    total_batches = math.ceil(len(rows) / batch_size)

    with driver.session() as session:
        for i in tqdm(range(total_batches), desc="Inserting data to Neo4j", unit="batch"):
            batch = rows[i * batch_size: (i + 1) * batch_size]
            session.run(query, parameters={"rows": batch})

    with driver.session() as session:
        session.run(query, parameters={"rows": rows})
