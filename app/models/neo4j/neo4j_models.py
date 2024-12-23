from dataclasses import dataclass
from datetime import datetime

@dataclass
class Group:
    name: str


@dataclass
class AttackType:
    name: str


@dataclass
class TargetType:
    name: str


@dataclass
class Location:
    region: str
    country: str
    city: str
    latitude: float
    longitude: float


@dataclass
class Attack:
    date: datetime
    group: Group
    target_type: TargetType
    attack_type: AttackType
    location: Location
