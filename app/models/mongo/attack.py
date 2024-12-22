from datetime import datetime

from pydantic import BaseModel, field_validator
from typing import List, Optional
from app.models.mongo.location import LocationModel
import re

class AttackModel(BaseModel):
    event_id: str
    date: datetime
    location: LocationModel
    attack_types: List[str]
    target_types: List[str]
    group_names: List[str]
    fatalities: Optional[int]
    injuries: Optional[int]
    terrorists_num: Optional[int]
    summary: Optional[str]

    @field_validator('date', mode='before')
    def validate_date(cls, v):
        if isinstance(v, str):
            if not re.match(r"\\d{4}-\\d{2}-\\d{2}", v):
                raise ValueError('Date must be in YYYY-MM-DD format.')
            return datetime.strptime(v, "%Y-%m-%d")
        elif isinstance(v, datetime):
            return v
        raise ValueError('Date must be a datetime object or a valid string in YYYY-MM-DD format.')

