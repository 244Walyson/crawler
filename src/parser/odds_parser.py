import re
from datetime import datetime
from typing import Any, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from selectolax.lexbor import LexborHTMLParser

from src.config.logging import logger
from src.models.event import EventStatus, Odds, SportEvent
from src.parser.normalizer import Normalizer


class OddsParser:
    """
    KISS-focused generic parser for multiple sports.
    """

    def __init__(self, allowed_domains: List[str]) -> None:
        self.allowed_domains = allowed_domains
        self.numeric_pattern = re.compile(r"(\d{1,2}\.\d{2})")
        self.sport_pattern = re.compile(
            r"(soccer|football|futebol|tennis|tenis|basketball|basquete|hockey|hoquei|baseball|volleyball|volei|handball|mma|ufc|boxing|boxe|rugby)", 
            re.IGNORECASE
        )

    def is_allowed(self, url: str) -> bool:
        for domain in self.allowed_domains:
            if f"//{domain}" in url or f".{domain}" in url:
                return True
        return False

    def _extract_from_container(self, container: Any, url: str, title: str) -> List[SportEvent]:
        raw_text = container.text(separator=" ", strip=True)
        
        # 1. Odds check (at least 2 numbers like 1.50)
        all_nums = [float(n) for n in self.numeric_pattern.findall(raw_text) if 1.01 <= float(n) <= 100.0]
        if len(all_nums) < 2:
            return []

        # 2. Team extraction (Target links/spans to get full names)
        potential_teams = []
        for node in container.css("a, span, div.team, td.name"):
            text = node.text(strip=True)
            if len(text) > 2 and not any(ch.isdigit() for ch in text) and "odds" not in text.lower():
                if text not in potential_teams:
                    potential_teams.append(text)
                    
        if len(potential_teams) < 2:
            return [] # Quality gate

        home, away = potential_teams[0], potential_teams[1]

        # 3. Sport Detection (URL > Title > Default)
        sport_match = self.sport_pattern.search(url) or self.sport_pattern.search(title)
        sport = sport_match.group(1).lower() if sport_match else "generic"
        
        # Standardize
        if sport in ["football", "futebol"]: sport = "soccer"
        if sport in ["basquete"]: sport = "basketball"
        if sport in ["volei"]: sport = "volleyball"
        if sport in ["tenis"]: sport = "tennis"

        odds = Odds(
            provider="Generic Aggregator",
            home_win=all_nums[0],
            draw=all_nums[1] if len(all_nums) >= 3 else None,
            away_win=all_nums[-1]
        )

        if Normalizer.validate_odds(odds):
            return [Normalizer.clean_event(SportEvent(
                sport=sport,
                tournament=title.split("|")[0].strip(),
                home_team=home,
                away_team=away,
                event_time=datetime.utcnow(),
                status=EventStatus.LIVE if "live" in url.lower() or "live" in raw_text.lower() else EventStatus.UPCOMING,
                url=url,
                odds=[odds]
            ))]
        
        return []

    def parse(self, html: str, url: str) -> Tuple[List[Tuple[int, str]], List[SportEvent]]:
        parser = LexborHTMLParser(html)
        title_node = parser.css_first("title")
        title = title_node.text() if title_node else ""
        
        # 1. Link Discovery
        new_urls = []
        for node in parser.css("a[href]"):
            link = node.attributes.get("href")
            if not link: continue
            abs_url = urljoin(url, link)
            if self.is_allowed(abs_url):
                new_urls.append((max(1, 10 - abs_url.count("/")), abs_url.split("#")[0]))

        # 2. Event Extraction
        events = []
        for container in parser.css("tr, div.match-row, div.event-row, div.custom-bet-block"):
            events.extend(self._extract_from_container(container, url, title))
        
        # Local deduplication
        unique = {}
        for e in events:
            unique[f"{e.sport}:{e.home_team}:{e.away_team}"] = e
            
        return new_urls, list(unique.values())
