from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Tuple

try:  # Python 3.11+
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef, attr-defined]


DEFAULT_TARGET_EXTENSIONS: Tuple[str, ...] = (
    '.pdf',
    '.zip',
    '.rar',
    '.7z',
    '.tar',
    '.tar.gz',
    '.doc',
    '.docx',
    '.docm',
    '.xls',
    '.xlsx',
    '.xlsm',
    '.csv',
    '.ppt',
    '.pptx',
    '.pptm',
    '.txt',
    '.rtf',
    '.jpg',
    '.jpeg',
    '.png',
    '.gif',
    '.bmp',
    '.svg',
    '.webp',
    '.tif',
    '.tiff',
)


@dataclass
class ScraperSettings:
    output_dir: Path = Path('downloads')
    logs_dir: Path = Path('logs')
    state_path: Path = Path('crawl_state.json')
    report_path: Path = Path('report.json')
    manifest_path: Path = Path('downloads_manifest.json')
    links_report_path: Path = Path('links_by_depth.json')
    user_agent: str = 'SimpleFileScraper/1.0 (+contactexample.com)'
    default_rate_limit: float = 2.0
    default_max_workers: int = 16
    default_download_workers: int = 20
    request_timeout: float = 20.0
    retry_attempts: int = 3
    retry_backoff_factor: float = 0.8
    allowed_content_types: tuple[str, ...] = ('text/html', 'application/xhtml+xml')
    target_extensions: tuple[str, ...] = DEFAULT_TARGET_EXTENSIONS

    include_patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)

    @classmethod
    def load(cls, path: Path | str) -> 'ScraperSettings':
        cfg_path = Path(path)
        if cfg_path.exists():
            data = tomllib.loads(cfg_path.read_text())
        else:
            data = {}
        kwargs = {key: data[key] for key in cls.__dataclass_fields__ if key in data}  # type: ignore[attr-defined]
        settings = cls(**kwargs)
        settings.output_dir = Path(settings.output_dir)
        settings.logs_dir = Path(settings.logs_dir)
        settings.state_path = Path(settings.state_path)
        settings.report_path = Path(settings.report_path)
        settings.manifest_path = Path(settings.manifest_path)
        settings.links_report_path = Path(settings.links_report_path)
        if not isinstance(settings.allowed_content_types, tuple):
            settings.allowed_content_types = tuple(settings.allowed_content_types)
        if not isinstance(settings.target_extensions, tuple):
            settings.target_extensions = tuple(settings.target_extensions)
        settings.include_patterns = list(settings.include_patterns)
        settings.exclude_patterns = list(settings.exclude_patterns)
        return settings

    def ensure_directories(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self.links_report_path.parent.mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> dict[str, object]:
        return {
            'output_dir': str(self.output_dir),
            'logs_dir': str(self.logs_dir),
            'state_path': str(self.state_path),
            'report_path': str(self.report_path),
            'manifest_path': str(self.manifest_path),
            'links_report_path': str(self.links_report_path),
            'user_agent': self.user_agent,
            'default_rate_limit': self.default_rate_limit,
            'default_max_workers': self.default_max_workers,
            'default_download_workers': self.default_download_workers,
            'request_timeout': self.request_timeout,
            'retry_attempts': self.retry_attempts,
            'retry_backoff_factor': self.retry_backoff_factor,
            'allowed_content_types': self.allowed_content_types,
            'target_extensions': self.target_extensions,
            'include_patterns': self.include_patterns,
            'exclude_patterns': self.exclude_patterns,
        }
