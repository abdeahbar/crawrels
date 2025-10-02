from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

ISO_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


@dataclass
class PageRecord:
    url: str
    depth: int
    title: Optional[str] = None
    assets: List[Dict[str, str]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    referrers: List[str] = field(default_factory=list)
    last_status: Optional[str] = None

    def to_dict(self) -> dict[str, object]:
        assets = list(self.assets)
        return {
            'url': self.url,
            'depth': self.depth,
            'title': self.title,
            'assets': assets,
            'pdfs': assets,
            'errors': list(self.errors),
            'referrers': list(self.referrers),
            'last_status': self.last_status,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> 'PageRecord':
        assets = payload.get('assets')
        if assets is None and 'pdfs' in payload:
            assets = payload.get('pdfs')
        referrers = payload.get('referrers') or payload.get('parents') or []
        return cls(
            url=payload['url'],
            depth=int(payload.get('depth', 0)),
            title=payload.get('title'),
            assets=list(assets or []),
            errors=list(payload.get('errors', [])),
            referrers=list(referrers or []),
            last_status=payload.get('last_status'),
        )


@dataclass
class CrawlState:
    start_url: str
    max_depth: int
    respect_robots: bool = True
    same_domain_only: bool = True
    include_patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)
    target_extensions: List[str] = field(default_factory=list)
    frontier: Deque[Tuple[str, int]] = field(default_factory=deque)
    visited: set[str] = field(default_factory=set)
    asset_cache: Dict[str, Dict[str, str]] = field(default_factory=dict)
    pages: Dict[str, PageRecord] = field(default_factory=dict)
    skipped: List[Dict[str, str]] = field(default_factory=list)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    referrers: Dict[str, List[str]] = field(default_factory=dict)
    asset_manifest: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if isinstance(self.frontier, list):  # pragma: no cover
            self.frontier = deque(self.frontier)
        if isinstance(self.visited, list):  # pragma: no cover
            self.visited = set(self.visited)
        if isinstance(self.target_extensions, tuple):  # pragma: no cover
            self.target_extensions = list(self.target_extensions)
        if isinstance(self.referrers, list):  # pragma: no cover
            # legacy state may serialise referrers as list pairs
            converted: Dict[str, List[str]] = {}
            for item in self.referrers:
                if isinstance(item, dict):
                    converted.update({str(k): list(v) for k, v in item.items()})
            self.referrers = converted

    @property
    def total_pages(self) -> int:
        return len(self.visited)

    @property
    def asset_count(self) -> int:
        return sum(len(page.assets) for page in self.pages.values())

    @property
    def pdf_count(self) -> int:
        # Backwards compatibility for callers expecting pdf_count
        return self.asset_count

    @property
    def pdf_cache(self) -> Dict[str, Dict[str, str]]:  # pragma: no cover - compatibility shim
        return self.asset_cache

    @pdf_cache.setter
    def pdf_cache(self, value: Dict[str, Dict[str, str]]) -> None:  # pragma: no cover - compatibility shim
        self.asset_cache = value

    def to_dict(self) -> dict[str, object]:
        cache = dict(self.asset_cache)
        return {
            'start_url': self.start_url,
            'max_depth': self.max_depth,
            'respect_robots': self.respect_robots,
            'same_domain_only': self.same_domain_only,
            'include_patterns': list(self.include_patterns),
            'exclude_patterns': list(self.exclude_patterns),
            'target_extensions': list(self.target_extensions),
            'frontier': list(self.frontier),
            'visited': list(self.visited),
            'asset_cache': cache,
            'pdf_cache': cache,
            'pages': {url: record.to_dict() for url, record in self.pages.items()},
            'skipped': list(self.skipped),
            'started_at': self.started_at,
            'finished_at': self.finished_at,
            'referrers': {url: list(refs) for url, refs in self.referrers.items()},
            'asset_manifest': self.asset_manifest,
        }

    def save(self, path: Path) -> None:
        payload = self.to_dict()
        path.write_text(json.dumps(payload, indent=2))

    @classmethod
    def load(cls, path: Path) -> 'CrawlState':
        raw = path.read_text()
        if not raw.strip():
            raise ValueError(f'State file {path} is empty')
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f'Invalid JSON in state file {path}: {exc}') from exc
        pages = {url: PageRecord.from_dict(data) for url, data in payload.get('pages', {}).items()}
        asset_cache = payload.get('asset_cache') or payload.get('pdf_cache') or {}
        state = cls(
            start_url=payload['start_url'],
            max_depth=int(payload['max_depth']),
            respect_robots=payload.get('respect_robots', True),
            same_domain_only=payload.get('same_domain_only', True),
            include_patterns=list(payload.get('include_patterns', [])),
            exclude_patterns=list(payload.get('exclude_patterns', [])),
            target_extensions=list(payload.get('target_extensions', [])),
            frontier=deque(tuple(item) for item in payload.get('frontier', [])),
            visited=set(payload.get('visited', [])),
            asset_cache=dict(asset_cache),
            pages=pages,
            skipped=list(payload.get('skipped', [])),
            started_at=payload.get('started_at'),
            finished_at=payload.get('finished_at'),
            referrers={url: list(refs) for url, refs in (payload.get('referrers') or {}).items()},
            asset_manifest=dict(payload.get('asset_manifest', {})),
        )
        return state

    def mark_started(self) -> None:
        if not self.started_at:
            self.started_at = datetime.utcnow().strftime(ISO_FORMAT)

    def mark_finished(self) -> None:
        self.finished_at = datetime.utcnow().strftime(ISO_FORMAT)

    def enqueue(self, url: str, depth: int) -> None:
        self.frontier.append((url, depth))

    def dequeue(self) -> Optional[Tuple[str, int]]:
        if not self.frontier:
            return None
        return self.frontier.popleft()

    def page(self, url: str, depth: int) -> PageRecord:
        record = self.pages.get(url)
        if record is None:
            record = PageRecord(url=url, depth=depth)
            self.pages[url] = record
        else:
            record.depth = min(record.depth, depth)
        for ref in self.referrers.get(url, []):
            if ref not in record.referrers:
                record.referrers.append(ref)
        return record

    def record_referrer(self, url: str, referrer: str) -> None:
        if not referrer:
            return
        refs = self.referrers.setdefault(url, [])
        if referrer not in refs:
            refs.append(referrer)
        if url in self.pages:
            page = self.pages[url]
            if referrer not in page.referrers:
                page.referrers.append(referrer)

    def register_asset(
        self,
        asset_url: str,
        path: str,
        page_url: str,
        depth: int,
        asset_type: Optional[str] = None,
        extension: Optional[str] = None,
        reused: bool = False,
    ) -> None:
        now = datetime.utcnow().strftime(ISO_FORMAT)
        entry = self.asset_manifest.get(asset_url)
        if entry is None:
            entry = {
                'path': path,
                'type': asset_type,
                'extension': extension,
                'first_page': page_url,
                'first_depth': depth,
                'first_seen': now,
                'pages': [],
            }
        else:
            entry['path'] = path
            if asset_type:
                entry['type'] = asset_type
            if extension:
                entry['extension'] = extension
        pages = entry.setdefault('pages', [])
        seen = False
        for item in pages:
            if item.get('page') == page_url:
                item.update({'depth': depth, 'path': path})
                if reused:
                    item['reused'] = True
                seen = True
                break
        if not seen:
            pages.append({'page': page_url, 'depth': depth, 'path': path, 'reused': reused})
        entry['last_seen'] = now
        entry['download_count'] = len(pages)
        self.asset_manifest[asset_url] = entry

    def record_skip(self, url: str, reason: str) -> None:
        self.skipped.append({'url': url, 'reason': reason})

    def to_report(self) -> dict[str, object]:
        return {
            'start_url': self.start_url,
            'max_depth': self.max_depth,
            'duration': self._duration(),
            'pages_visited': self.total_pages,
            'asset_count': self.asset_count,
            'pdf_count': self.asset_count,
            'pages': {url: record.to_dict() for url, record in self.pages.items()},
            'skipped': list(self.skipped),
        }

    def build_links_by_depth(self) -> dict[str, object]:
        levels: Dict[int, List[Dict[str, Any]]] = {}
        for url, record in self.pages.items():
            bucket = levels.setdefault(record.depth, [])
            bucket.append(
                {
                    'url': url,
                    'title': record.title,
                    'referrers': list(record.referrers),
                    'asset_count': len(record.assets),
                    'assets': list(record.assets),
                }
            )
        ordered = [
            {'depth': depth, 'pages': sorted(pages, key=lambda item: item['url'])}
            for depth, pages in sorted(levels.items(), key=lambda item: item[0])
        ]
        return {
            'start_url': self.start_url,
            'generated_at': datetime.utcnow().strftime(ISO_FORMAT),
            'levels': ordered,
        }

    def build_asset_manifest(self) -> dict[str, Any]:
        return {
            'start_url': self.start_url,
            'generated_at': datetime.utcnow().strftime(ISO_FORMAT),
            'assets': self.asset_manifest,
        }

    def _duration(self) -> Optional[float]:
        if not self.started_at:
            return None
        end = self.finished_at or datetime.utcnow().strftime(ISO_FORMAT)
        start_dt = datetime.strptime(self.started_at, ISO_FORMAT)
        end_dt = datetime.strptime(end, ISO_FORMAT)
        return (end_dt - start_dt).total_seconds()
