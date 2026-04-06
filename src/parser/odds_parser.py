import re
from datetime import datetime
from typing import Any, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from selectolax.lexbor import LexborHTMLParser

from src.config.logging import logger
from src.models.event import Odds, SportEvent
from src.parser.normalizer import Normalizer


class OddsParser:
    """
    Highly efficient generic parser for sports betting sites.
    Uses pre-compiled patterns and fail-fast checks.
    """

    def __init__(self, allowed_domains: List[str]) -> None:
        self.allowed_domains = allowed_domains
        # Pre-compile patterns for speed
        self.numeric_pattern = re.compile(r"(\d{1,2}\.\d{2})")
        # Fail-fast check: does row have at least one digit?
        self.has_digit = re.compile(r"\d")
        self.noise_pattern = re.compile(r"(tabela|resultados|odds|futebol|vs|v|x|hoje|live|rodada|classific)", re.IGNORECASE)

    def is_allowed(self, url: str) -> bool:
        # Fast path string matching instead of full urlparse
        for domain in self.allowed_domains:
            if f"//{domain}" in url or f".{domain}" in url:
                return True
        return False

    def _extract_from_container(self, container: Any, url: str, parser: LexborHTMLParser) -> List[SportEvent]:
        # Optimization: Fail-fast if container text has no digits (no odds)
        raw_text = container.text(separator=" ", strip=True)
        if not self.has_digit.search(raw_text):
            return []
        
        # Look for numeric values
        all_nums = [float(n) for n in self.numeric_pattern.findall(raw_text) if 1.01 <= float(n) <= 100.0]
        if len(all_nums) < 2:
            return []

        # Extract team names efficiently
        potential_teams = []
        for word in raw_text.split():
            # Strip non-alpha
            clean_word = "".join(ch for ch in word if ch.isalpha()).strip()
            if len(clean_word) > 2 and not self.noise_pattern.search(clean_word):
                potential_teams.append(clean_word)

        if len(potential_teams) >= 2:
            home, away = potential_teams[0], potential_teams[1]
            
            # Simple tournament extraction
            tournament = "Discovery"
            title_node = parser.css_first("title")
            if title_node:
                tournament = title_node.text().split("|")[0].split("-")[0].strip()

            odds = Odds(
                provider="Generic Aggregator",
                home_win=all_nums[0],
                draw=all_nums[1] if len(all_nums) >= 3 else None,
                away_win=all_nums[-1]
            )

            if Normalizer.validate_odds(odds):
                event = SportEvent(
                    sport="soccer",
                    tournament=tournament,
                    home_team=home,
                    away_team=away,
                    event_time=datetime.utcnow(),
                    url=url,
                    odds=[odds]
                )
                return [Normalizer.clean_event(event)]
        
        return []

    def parse(self, html: str, url: str) -> Tuple[List[Tuple[int, str]], List[SportEvent]]:
        parser = LexborHTMLParser(html)
        new_urls: List[Tuple[int, str]] = []
        events: List[SportEvent] = []

        # 1. Efficient Link Discovery
        # We only iterate a[href] once
        for node in parser.css("a[href]"):
            link = node.attributes.get("href")
            if not link: continue
            
            absolute_url = urljoin(url, link)
            if self.is_allowed(absolute_url):
                clean_url = absolute_url.split("#")[0].split("?")[0]
                # Heuristic: deep paths = higher priority
                depth = clean_url.count("/")
                priority = max(1, 10 - depth)
                new_urls.append((priority, clean_url))

        # 2. Optimized structural extraction
        # Only check containers likely to hold data
        for container in parser.css("tr, div.match-row, div.event-row"):
            extracted = self._extract_from_container(container, url, parser)
            if extracted:
                events.extend(extracted)
        
        # Fast local deduplication
        unique_events = {}
        for e in events:
            key = f"{e.home_team}:{e.away_team}"
            if key not in unique_events:
                unique_events[key] = e
        
        return new_urls, list(unique_events.values())
