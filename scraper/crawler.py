from __future__ import annotations

import json
import logging
import os
import shutil
import threading
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse
from typing import Deque, Dict, Iterable, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import utils
from .config import ScraperSettings
from .rate_limit import RateLimiter
from .robots import RobotsHandler
from .state import CrawlState, PageRecord

LOGGER = logging.getLogger(__name__)


class CrawlEvent:
    def __init__(self, action: str, url: str, detail: str) -> None:
        self.timestamp = time.time()
        self.action = action
        self.url = url
        self.detail = detail

    def to_dict(self) -> dict[str, object]:
        return {
            'timestamp': self.timestamp,
            'action': self.action,
            'url': self.url,
            'detail': self.detail,
        }


class FileCrawler:
    def __init__(
        self,
        start_url: str,
        max_depth: int,
        settings: ScraperSettings,
        concurrency: int = 16,
        rate_limit: float = 2.0,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        respect_robots: bool = True,
        same_domain_only: bool = True,
        resume: bool = False,
        download_concurrency: Optional[int] = None,
        target_extensions: Optional[Iterable[str]] = None,
    ) -> None:
        self.settings = settings
        self.settings.ensure_directories()
        self.concurrency = max(1, concurrency)
        desired_downloads = download_concurrency or settings.default_download_workers
        self.download_concurrency = max(1, desired_downloads)
        self.target_extensions = self._prepare_extensions(target_extensions or settings.target_extensions)
        self.rate_limiter = RateLimiter(rate_limit)
        self.session = self._build_session(settings)
        self.robots = RobotsHandler(settings.user_agent, respect=respect_robots)

        self._pause_event = threading.Event()
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._recent_events: Deque[CrawlEvent] = deque(maxlen=100)
        self._active_futures: set[Future] = set()
        self._active_downloads = 0
        self._download_pool: Optional[ThreadPoolExecutor] = None
        self._crawl_pool: Optional[ThreadPoolExecutor] = None
        self._thread: Optional[threading.Thread] = None
        self._asset_inflight: Dict[str, Future] = {}
        self._asset_waiters: Dict[str, List[PageRecord]] = {}

        loaded = False
        if resume and settings.state_path.exists():
            try:
                self.state = CrawlState.load(settings.state_path)
                LOGGER.info('Resuming existing crawl state with %s pages visited.', len(self.state.visited))
                loaded = True
            except ValueError as exc:
                LOGGER.warning('Failed to load crawl state %s: %s. Starting fresh.', settings.state_path, exc)
        if not loaded:
            self.state = CrawlState(
                start_url=start_url,
                max_depth=max_depth,
                respect_robots=respect_robots,
                same_domain_only=same_domain_only,
                include_patterns=list(include_patterns or []),
                exclude_patterns=list(exclude_patterns or []),
                target_extensions=list(self.target_extensions),
            )
            self.state.enqueue(start_url, 0)
            self._hydrate_cache_from_previous_run(settings.state_path)
        self.state.mark_started()

        if not self.state.target_extensions:
            self.state.target_extensions = list(self.target_extensions)
        else:
            self.target_extensions = self._prepare_extensions(self.state.target_extensions)

        self._queued_pages: set[str] = {url for url, _ in self.state.frontier}
        self._status_snapshot: Dict[str, object] = {}
        self._update_status('initialized')

    @staticmethod
    def _prepare_extensions(raw: Iterable[str]) -> Tuple[str, ...]:
        normalized: Set[str] = set()
        for ext in raw:
            if not ext:
                continue
            cleaned = ext.lower().strip()
            if not cleaned:
                continue
            if not cleaned.startswith('.'):
                cleaned = f'.{cleaned}'
            normalized.add(cleaned)
        if not normalized:
            normalized.add('.pdf')
        return tuple(sorted(normalized))

    def _hydrate_cache_from_previous_run(self, state_path: Path) -> None:
        if not state_path.exists():
            return
        try:
            payload = json.loads(state_path.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            LOGGER.debug('Unable to hydrate cache from %s: %s', state_path, exc)
            return
        cached_assets = payload.get('asset_cache') or payload.get('pdf_cache') or {}
        manifest = payload.get('asset_manifest') or {}
        with self._lock:
            for url, entry in cached_assets.items():
                if url not in self.state.asset_cache and isinstance(entry, dict) and entry.get('path'):
                    self.state.asset_cache[url] = entry
            for url, entry in manifest.items():
                if url not in self.state.asset_manifest and isinstance(entry, dict):
                    self.state.asset_manifest[url] = entry

    def _build_session(self, settings: ScraperSettings) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=settings.retry_attempts,
            backoff_factor=settings.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=['HEAD', 'GET'],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({'User-Agent': settings.user_agent})
        return session

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._pause_event.clear()
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name='CrawlerMain', daemon=True)
        self._thread.start()
        self._update_status('running')

    def pause(self) -> None:
        self._pause_event.set()
        self._update_status('paused')

    def resume(self) -> None:
        if not self._thread or not self._thread.is_alive():
            self.start()
            return
        self._pause_event.clear()
        self._update_status('running')

    def stop(self) -> None:
        self._stop_event.set()
        self._pause_event.clear()
        if self._thread:
            self._thread.join(timeout=5)
        self.state.mark_finished()
        self._persist()
        self._write_report()
        self._update_status('stopped')

    def _run(self) -> None:
        LOGGER.info('Crawler started. Frontier size: %s', len(self.state.frontier))
        with ThreadPoolExecutor(max_workers=self.concurrency, thread_name_prefix='crawl') as crawl_pool:
            with ThreadPoolExecutor(max_workers=self.download_concurrency, thread_name_prefix='download') as download_pool:
                self._crawl_pool = crawl_pool
                self._download_pool = download_pool
                try:
                    self._crawl_loop()
                finally:
                    LOGGER.info('Crawler loop finished. Writing results...')
                    self.state.mark_finished()
                    self._persist()
                    self._write_report()
                    self._update_status('finished')

    def _crawl_loop(self) -> None:
        while not self._stop_event.is_set():
            if self._pause_event.is_set():
                time.sleep(0.3)
                continue

            task = self.state.dequeue()
            if task is None:
                if not self._active_futures and self._active_downloads == 0:
                    LOGGER.info('No more tasks; crawl loop exiting.')
                    break
                time.sleep(0.2)
                continue

            url, depth = task
            with self._lock:
                if url in self.state.visited:
                    continue
                self.state.visited.add(url)
                self._queued_pages.discard(url)
            future = self._crawl_pool.submit(self._process_page, url, depth)
            self._active_futures.add(future)
            future.add_done_callback(self._future_done)

    def _future_done(self, future: Future) -> None:
        self._active_futures.discard(future)
        exc = future.exception()
        if exc:
            LOGGER.error('Worker raised exception: %s', exc)
            self._record_event('error', 'worker', str(exc))
        self._persist()
        self._update_status('running')

    def _process_page(self, url: str, depth: int) -> None:
        LOGGER.info('Fetching page %s (depth %s)', url, depth)
        self.rate_limiter.acquire()
        try:
            response = self.session.get(url, timeout=self.settings.request_timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            LOGGER.warning('Failed to fetch %s: %s', url, exc)
            with self._lock:
                self.state.page(url, depth).errors.append(f'fetch-error: {exc}')
            self._record_event('page_error', url, str(exc))
            return

        content_type = response.headers.get('Content-Type', '')
        if not any(ct in content_type for ct in self.settings.allowed_content_types):
            LOGGER.debug('Skipping non-html content at %s: %s', url, content_type)
            return

        soup = BeautifulSoup(response.text, 'html.parser')
        title_tag = soup.find('title')
        title = title_tag.text.strip() if title_tag else None

        page_record = self.state.page(url, depth)
        page_record.title = title
        page_record.last_status = 'fetched'

        self._record_event('page', url, 'fetched')

        links = self._extract_links(url, soup)
        asset_links = links['assets']
        html_links = links['html']

        if asset_links:
            folder = utils.build_page_folder(self.settings.output_dir, url, title)
            for asset_url, label, extension in asset_links:
                self._schedule_asset_download(page_record, asset_url, folder, label, extension)

        if depth >= self.state.max_depth:
            return

        for link in html_links:
            self._enqueue_page(url, link, depth + 1)

    def _extract_links(self, base_url: str, soup: BeautifulSoup) -> Dict[str, List]:
        assets: List[Tuple[str, str, str]] = []
        html_links: List[str] = []
        assets_seen: Set[str] = set()
        html_seen: Set[str] = set()
        target_exts = self.state.target_extensions or list(self.target_extensions)

        def consider(url_value: Optional[str], label: str = '') -> None:
            if not url_value:
                return
            norm = utils.normalize_url(base_url, url_value)
            if not norm:
                return
            if utils.match_patterns(norm, self.state.exclude_patterns):
                self._record_skip(norm, 'exclude-pattern')
                return
            if self.state.include_patterns and not utils.match_patterns(norm, self.state.include_patterns):
                self._record_skip(norm, 'include-miss')
                return
            if self.state.same_domain_only and not utils.same_registrable_domain(norm, self.state.start_url):
                self._record_skip(norm, 'off-domain')
                return
            if not self.robots.is_allowed(norm):
                self._record_skip(norm, 'robots')
                return
            extension = utils.match_extension(norm, target_exts)
            if extension:
                if norm not in assets_seen:
                    assets_seen.add(norm)
                    assets.append((norm, label, extension))
            else:
                if norm not in html_seen:
                    html_seen.add(norm)
                    html_links.append(norm)

        for anchor in soup.find_all('a', href=True):
            consider(anchor['href'], anchor.get_text(strip=True) or '')
        for tag in soup.find_all(['iframe', 'embed', 'object']):
            for attr in ('src', 'data'):
                if tag.has_attr(attr):
                    consider(tag[attr], tag.get('title') or '')
        for link_tag in soup.find_all('link', href=True):
            consider(link_tag['href'], link_tag.get('title') or link_tag.get('rel', [''])[0])
        for img in soup.find_all('img'):
            consider(img.get('src'), img.get('alt') or '')
            srcset = img.get('srcset')
            if srcset:
                for part in srcset.split(','):
                    candidate = part.strip().split(' ')[0]
                    consider(candidate, img.get('alt') or '')
        for source in soup.find_all('source'):
            consider(source.get('src'), source.get('title') or '')
            srcset = source.get('srcset')
            if srcset:
                for part in srcset.split(','):
                    candidate = part.strip().split(' ')[0]
                    consider(candidate, source.get('title') or '')

        return {'assets': assets, 'html': html_links}

    def _enqueue_page(self, parent_url: str, url: str, depth: int) -> None:
        with self._lock:
            self.state.record_referrer(url, parent_url)
            if url in self.state.visited or url in self._queued_pages:
                return
            self.state.enqueue(url, depth)
            self._queued_pages.add(url)
        self._record_event('enqueue', url, f'parent={parent_url} depth {depth}')

    def _schedule_asset_download(self, page: PageRecord, asset_url: str, folder: Path, label: str, extension: str) -> None:
        cache_entry: Optional[Dict[str, str]] = None
        with self._lock:
            if any(item.get('url') == asset_url for item in page.assets):
                return
            entry = self.state.asset_cache.get(asset_url)
            if entry:
                source_path = Path(entry['path'])
                if source_path.exists():
                    cache_entry = dict(entry)
                else:
                    self.state.asset_cache.pop(asset_url, None)

        if cache_entry:
            reused = self._materialize_cached_asset(page, asset_url, folder, label, extension, cache_entry)
            if reused:
                return

        with self._lock:
            if page in self._asset_waiters.get(asset_url, []):
                return
            if asset_url in self._asset_inflight:
                self._asset_waiters.setdefault(asset_url, []).append(page)
                return
            if any(item.get('url') == asset_url for item in page.assets):
                return
            self._active_downloads += 1

        if not self._download_pool:
            raise RuntimeError('Download pool not available')
        future = self._download_pool.submit(self._download_asset, page, asset_url, folder, label, extension)
        with self._lock:
            self._asset_inflight[asset_url] = future
        future.add_done_callback(self._download_done)
    def _attach_asset_to_page(
        self,
        page: PageRecord,
        asset_url: str,
        path: str,
        asset_type: Optional[str],
        extension: Optional[str],
        reused: bool,
    ) -> None:
        entry = {'url': asset_url, 'path': path}
        if asset_type:
            entry['type'] = asset_type
        if extension:
            entry['extension'] = extension
        if reused:
            entry['reused'] = True
        with self._lock:
            for existing in page.assets:
                if existing.get('url') == asset_url and existing.get('path') == path:
                    if reused and not existing.get('reused'):
                        existing['reused'] = True
                    return
            page.assets.append(entry)
            cache_entry = {'path': path}
            if asset_type:
                cache_entry['type'] = asset_type
            if extension:
                cache_entry['extension'] = extension
            self.state.asset_cache[asset_url] = cache_entry
            self.state.register_asset(
                asset_url,
                path,
                page.url,
                page.depth,
                asset_type,
                extension,
                reused=reused,
            )

    def _drain_asset_waiters(self, asset_url: str) -> List[PageRecord]:
        with self._lock:
            return self._asset_waiters.pop(asset_url, [])

    def _materialize_cached_asset(
        self,
        page: PageRecord,
        asset_url: str,
        folder: Path,
        label: str,
        extension: str,
        cache_entry: Dict[str, str],
    ) -> bool:
        source_path = Path(cache_entry.get('path', ''))
        if not source_path.exists():
            LOGGER.debug('Cached asset missing on disk %s', cache_entry.get('path'))
            with self._lock:
                self.state.asset_cache.pop(asset_url, None)
            return False

        asset_type = cache_entry.get('type')
        preferred_ext = cache_entry.get('extension') or extension
        self._attach_asset_to_page(
            page,
            asset_url,
            str(source_path),
            asset_type,
            preferred_ext,
            reused=True,
        )
        self._record_event('asset_reuse', asset_url, str(source_path))
        return True
    def _download_asset(self, page: PageRecord, asset_url: str, folder: Path, label: str, extension: str) -> None:
        LOGGER.info('Downloading asset %s', asset_url)
        slug = utils.slugify_title(label) if label else ''
        parsed_path = Path(urlparse(asset_url).path)
        fallback_base = slug or parsed_path.stem or 'file'
        fallback = f"{fallback_base}{extension}" if extension else fallback_base
        filename = utils.filename_from_url(asset_url, fallback)
        lower_ext = extension.lower() if extension else ''
        if lower_ext and not filename.lower().endswith(lower_ext):
            filename = f"{filename}{lower_ext}"
        target_path = folder / filename
        suffix = 1
        suffix_ext = lower_ext or ''.join(parsed_path.suffixes) or Path(filename).suffix
        if suffix_ext and not suffix_ext.startswith('.'):
            suffix_ext = f'.{suffix_ext}'
        base_stem = Path(filename).stem or 'file'
        while target_path.exists():
            target_path = folder / f"{base_stem}_{suffix}{suffix_ext}"
            suffix += 1

        head_content_type = None
        try:
            self.rate_limiter.acquire()
            head_resp = self.session.head(asset_url, timeout=self.settings.request_timeout, allow_redirects=True)
            head_resp.raise_for_status()
            head_content_type = head_resp.headers.get('Content-Type')
        except requests.RequestException:
            head_content_type = None

        response = None
        try:
            self.rate_limiter.acquire()
            response = self.session.get(asset_url, stream=True, timeout=self.settings.request_timeout)
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '').lower()
            if 'text/html' in content_type:
                raise ValueError('content-type html')
            folder.mkdir(parents=True, exist_ok=True)
            with open(target_path, 'wb') as handle:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        handle.write(chunk)
            asset_type = head_content_type or content_type or None
            file_extension = extension or Path(target_path).suffix
            self._attach_asset_to_page(
                page,
                asset_url,
                str(target_path),
                asset_type,
                file_extension,
                reused=False,
            )
            self._record_event('asset', asset_url, str(target_path))
            waiters = self._drain_asset_waiters(asset_url)
            for waiter_page in waiters:
                self._attach_asset_to_page(
                    waiter_page,
                    asset_url,
                    str(target_path),
                    asset_type,
                    file_extension,
                    reused=True,
                )
                self._record_event('asset_reuse', asset_url, str(target_path))
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning('Failed to download asset %s: %s', asset_url, exc)
            with self._lock:
                page.errors.append(f'download-error: {exc}')
                self.state.asset_cache.pop(asset_url, None)
            waiters = self._drain_asset_waiters(asset_url)
            for waiter_page in waiters:
                with self._lock:
                    waiter_page.errors.append(f'download-error: {exc}')
            if target_path.exists():
                try:
                    target_path.unlink()
                except OSError:
                    pass
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass
    def _download_done(self, future: Future) -> None:
        asset_url = None
        with self._lock:
            for url, inflight_future in list(self._asset_inflight.items()):
                if inflight_future is future:
                    asset_url = url
                    del self._asset_inflight[url]
                    break
            self._active_downloads = max(0, self._active_downloads - 1)
        exc = future.exception()
        if exc:
            if asset_url:
                LOGGER.error('Asset download failed for %s: %s', asset_url, exc)
            else:
                LOGGER.error('Asset download failed: %s', exc)
        self._persist()
        self._update_status('running')
    def _record_skip(self, url: str, reason: str) -> None:
        with self._lock:
            self.state.record_skip(url, reason)

    def _record_event(self, action: str, url: str, detail: str) -> None:
        with self._lock:
            self._recent_events.appendleft(CrawlEvent(action, url, detail))

    def _persist(self) -> None:
        with self._lock:
            self.state.save(self.settings.state_path)

    def _write_report(self) -> None:
        report = self.state.to_report()
        self.settings.report_path.write_text(json.dumps(report, indent=2))
        manifest = self.state.build_asset_manifest()
        self.settings.manifest_path.write_text(json.dumps(manifest, indent=2))
        link_map = self.state.build_links_by_depth()
        self.settings.links_report_path.write_text(json.dumps(link_map, indent=2))

    def _update_status(self, status: str) -> None:
        with self._lock:
            self._status_snapshot = {
                'status': status,
                'pages_visited': len(self.state.visited),
                'frontier_size': len(self.state.frontier),
                'asset_count': self.state.asset_count,
                'unique_assets': len(self.state.asset_manifest),
                'downloads_inflight': self._active_downloads,
                'target_extensions': list(self.target_extensions),
                'events': [event.to_dict() for event in list(self._recent_events)[:10]],
            }

    def get_status(self) -> Dict[str, object]:
        with self._lock:
            return dict(self._status_snapshot)

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive() and not self._pause_event.is_set()

    def await_completion(self, timeout: Optional[float] = None) -> None:
        if self._thread:
            self._thread.join(timeout)
