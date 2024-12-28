from typing import List
from bson import ObjectId
from app.models.mongo.attack import AttackModel


def insert_attack(collection, attack: AttackModel):
    return collection.insert_one(attack.model_dump()).inserted_id

def insert_many_attacks(collection, attacks: List[AttackModel]):
    return collection.insert_many(
        [attack.model_dump() for attack in attacks if attack is not None]
    ).inserted_ids

def get_attack_by_id(collection, attack_id: str):
    return collection.find_one({"_id": ObjectId(attack_id)})

def get_all_attacks(collection):
    return list(collection.find())

def delete_attack_by_id(collection, attack_id: str):
    return collection.delete_one({"_id": ObjectId(attack_id)}).deleted_count

def delete_all_attacks(collection):
    return collection.delete_many({}).deleted_count
