from pydantic import BaseModel
from typing import Optional

class SkillUpdateRequest(BaseModel):
    workday_skill: str
    lightcast_skill: str
