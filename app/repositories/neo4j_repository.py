from dataclasses import asdict
from typing import List
from app.db.neo4j_database import driver
from app.models.neo4j.attack import Attack


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

    rows = [convert_to_dict(attack) for attack in attacks if isinstance(attack, Attack)]
    with driver.session() as session:
        session.run(query, parameters={"rows": rows})

def convert_to_dict(attack: Attack):
    attack_dict = asdict(attack)
    attack_dict["date"] = attack.date.isoformat()
    attack_dict["group"] = {"name": attack.group.name}
    attack_dict["target_type"] = {"name": attack.target_type.name}
    attack_dict["attack_type"] = {"name": attack.attack_type.name}
    attack_dict["location"] = {
        "region": attack.location.region,
        "country": attack.location.country,
        "latitude": attack.location.latitude,
        "longitude": attack.location.longitude,
    }
    return attack_dict
