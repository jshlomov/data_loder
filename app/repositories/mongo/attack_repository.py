from pymongo import MongoClient
from typing import List
from bson import ObjectId
from app.models.mongo.attack import AttackModel

class AttackRepository:
    def __init__(self, db_url: str, db_name: str, collection_name: str):
        self.client = MongoClient(db_url)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def insert_attack(self, attack: AttackModel):
        return self.collection.insert_one(attack.model_dump()).inserted_id

    def insert_many_attacks(self, attacks: List[AttackModel]):
        return self.collection.insert_many(
            [attack.model_dump() for attack in attacks if attack is not None]
        ).inserted_ids

    def get_attack_by_id(self, attack_id: str):
        return self.collection.find_one({"_id": ObjectId(attack_id)})

    def get_all_attacks(self):
        return list(self.collection.find())

    def delete_attack_by_id(self, attack_id: str):
        return self.collection.delete_one({"_id": ObjectId(attack_id)}).deleted_count

    def delete_all_attacks(self):
        return self.collection.delete_many({}).deleted_count
