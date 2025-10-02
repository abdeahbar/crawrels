from __future__ import annotations

import logging
from functools import lru_cache
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser


class RobotsHandler:
    def __init__(self, user_agent: str, respect: bool = True) -> None:
        self.user_agent = user_agent
        self.respect = respect
        self._logger = logging.getLogger(__name__)

    def is_allowed(self, url: str) -> bool:
        if not self.respect:
            return True
        parser = self._parser_for(url)
        if parser is None:
            return True
        return parser.can_fetch(self.user_agent, url)

    @lru_cache(maxsize=32)
    def _parser_for(self, url: str) -> RobotFileParser | None:
        parsed = urlparse(url)
        robots_url = urljoin(f'{parsed.scheme}://{parsed.netloc}', '/robots.txt')
        parser = RobotFileParser()
        try:
            parser.set_url(robots_url)
            parser.read()
            return parser
        except Exception as exc:  # noqa: BLE001
            self._logger.warning('Failed to read robots.txt %s: %s', robots_url, exc)
            return None
