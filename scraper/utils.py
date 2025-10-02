from __future__ import annotations

import hashlib
import re
import unicodedata
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse, urlunparse

import tldextract

EXTRACTOR = tldextract.TLDExtract(suffix_list_urls=None)
INVALID_FS_CHARS = set('<>:"/\\|?*')


def normalize_url(base: str, href: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith('mailto:') or href.startswith('javascript:'):
        return None
    joined = urljoin(base, href)
    parsed = urlparse(joined)
    if parsed.scheme not in {'http', 'https'}:
        return None
    clean = parsed._replace(fragment='')
    normalized = urlunparse(clean)
    return normalized.rstrip('/') if clean.path != '/' else normalized


def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    ext = EXTRACTOR(parsed.netloc)
    return '.'.join(part for part in (ext.domain, ext.suffix) if part)


def same_registrable_domain(url: str, reference: str) -> bool:
    return extract_domain(url) == extract_domain(reference)


def sanitize_for_fs(text: str, replacement: str = '_') -> str:
    sanitized = ''.join(replacement if ch in INVALID_FS_CHARS else ch for ch in text)
    sanitized = sanitized.strip()
    sanitized = re.sub(r'\s+', replacement, sanitized)
    sanitized = sanitized[:150]
    return sanitized or 'item'


def slugify_title(title: str) -> str:
    normalized = unicodedata.normalize('NFKC', title).strip()
    if not normalized:
        return 'page'
    cleaned = re.sub(r'[^\w\s-]', ' ', normalized, flags=re.UNICODE)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip().lower()
    if not cleaned:
        return 'page'
    slug = cleaned.replace(' ', '-')
    slug = re.sub(r'-{2,}', '-', slug)
    return slug[:120] or 'page'


def build_page_folder(base_dir: Path, page_url: str, title: Optional[str] = None) -> Path:
    parsed = urlparse(page_url)
    segments = [sanitize_for_fs(parsed.netloc)]
    path_parts = [part for part in parsed.path.split('/') if part]
    if path_parts:
        segments.extend(sanitize_for_fs(part) for part in path_parts)
    else:
        segments.append('root')
    if parsed.query:
        segments.append(sanitize_for_fs(parsed.query))
    if title:
        segments.append(sanitize_for_fs(slugify_title(title)))
    else:
        segments.append('index')
    full_path = base_dir.joinpath(*segments)
    return full_path


def hash_stream(stream_iterable: Iterable[bytes]) -> str:
    digest = hashlib.sha1()
    for chunk in stream_iterable:
        if chunk:
            digest.update(chunk)
    return digest.hexdigest()


def filename_from_url(url: str, fallback: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name
    if name:
        suffix = Path(name).suffix
        if suffix:
            return sanitize_for_fs(name)
    return sanitize_for_fs(fallback)


def match_patterns(value: str, patterns: Iterable[str]) -> bool:
    for pattern in patterns:
        if not pattern:
            continue
        try:
            if re.search(pattern, value):
                return True
        except re.error:
            if pattern in value:
                return True
    return False


def match_extension(url: str, extensions: Iterable[str]) -> Optional[str]:
    path = urlparse(url).path.lower()
    normalized = [ext.lower() for ext in extensions if ext]
    for ext in sorted(set(normalized), key=len, reverse=True):
        if path.endswith(ext):
            return ext
    return None
