import re
from typing import Any, Dict, Optional

from src.models.event import SportEvent, Odds


class Normalizer:
    """Handles Team/League Normalization and Data Validation."""

    # Simple normalization map (In reality, this would be a larger DB)
    TEAM_MAP = {
        "Man Utd": "Manchester United",
        "Man City": "Manchester City",
        "Atleti": "Atletico Madrid",
    }

    @classmethod
    def normalize_name(cls, name: str) -> str:
        """Standardize team and tournament names."""
        name = name.strip()
        return cls.TEAM_MAP.get(name, name)

    @classmethod
    def validate_odds(cls, odds: Odds) -> bool:
        """Ensure odds are logical (> 1.0) and consistent."""
        if odds.home_win <= 1.0 or odds.away_win <= 1.0:
            return False
        if odds.draw and odds.draw <= 1.0:
            return False
        
        # Check for extreme anomalies (e.g., odds > 1000 might be errors)
        if any(v > 1000 for v in [odds.home_win, odds.away_win]):
            return False
            
        return True

    @classmethod
    def clean_event(cls, event: SportEvent) -> SportEvent:
        """Normalize all entity names in an event."""
        event.home_team = cls.normalize_name(event.home_team)
        event.away_team = cls.normalize_name(event.away_team)
        event.tournament = cls.normalize_name(event.tournament)
        return event
