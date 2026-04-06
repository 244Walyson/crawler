from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class EventStatus(str, Enum):
    UPCOMING = "upcoming"
    LIVE = "live"
    FINISHED = "finished"
    CANCELLED = "cancelled"


class Odds(BaseModel):
    provider: str
    home_win: float
    draw: Optional[float] = None
    away_win: float
    market_type: str = "1x2"
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class SportEvent(BaseModel):
    event_id: Optional[str] = None
    sport: str
    tournament: str
    home_team: str
    away_team: str
    event_time: datetime
    status: EventStatus = EventStatus.UPCOMING
    url: str
    odds: List[Odds] = []
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = {}

    def calculate_id(self) -> str:
        import hashlib
        key = f"{self.sport}:{self.tournament}:{self.home_team}:{self.away_team}"
        return hashlib.md5(key.encode()).hexdigest()
