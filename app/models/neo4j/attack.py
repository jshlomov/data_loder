from dataclasses import dataclass
from datetime import datetime

from app.models.neo4j.attack_type import AttackType
from app.models.neo4j.group import Group
from app.models.neo4j.location import Location
from app.models.neo4j.target_type import TargetType


@dataclass
class Attack:
    date: datetime
    group: Group
    target_type: TargetType
    attack_type: AttackType
    location: Location