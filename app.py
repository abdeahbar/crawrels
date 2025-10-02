from __future__ import annotations

import time
from pathlib import Path
from typing import Iterable, List

import streamlit as st

from scraper import FileCrawler, ScraperSettings
from scraper.logger import configure_logging

CONFIG_PATH = Path('config.toml')
settings = ScraperSettings.load(CONFIG_PATH)
configure_logging(settings.logs_dir / 'scraper.log')
settings.ensure_directories()

st.set_page_config(page_title='Website File Scraper', layout='wide')

if 'crawler' not in st.session_state:
    st.session_state.crawler = None
if 'status' not in st.session_state:
    st.session_state.status = {}


def normalize_ext_values(values: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for value in values:
        clean = value.strip().lower()
        if not clean:
            continue
        if not clean.startswith('.'):
            clean = f'.{clean}'
        if clean not in seen:
            seen.add(clean)
            normalized.append(clean)
    return normalized


st.sidebar.header('Crawler Settings')
start_url = st.sidebar.text_input('Start URL', value='')
max_depth = st.sidebar.number_input('Max depth', min_value=0, max_value=10, value=3, step=1)
include_patterns_raw = st.sidebar.text_area('Include patterns (regex, one per line)', '')
exclude_patterns_raw = st.sidebar.text_area('Exclude patterns (regex, one per line)', '')
concurrency = st.sidebar.slider('Page workers', 1, 128, value=min(settings.default_max_workers, 128))
download_concurrency = st.sidebar.slider('Concurrent downloads', 1, 128, value=min(settings.default_download_workers, 128))
rate_limit = st.sidebar.slider('Rate limit (requests/sec)', 0.0, 50.0, value=min(settings.default_rate_limit, 50.0), step=0.5)
respect_robots = st.sidebar.checkbox('Respect robots.txt', value=True)
same_domain = st.sidebar.checkbox('Same-domain only', value=True)
resume_existing = st.sidebar.checkbox('Resume from saved state', value=False)

extension_choices = sorted({ext.lower() for ext in settings.target_extensions})
selected_exts = st.sidebar.multiselect('File types to download', extension_choices, default=extension_choices)
extra_exts_raw = st.sidebar.text_input('Additional extensions (comma separated)', value='')
extra_exts = [ext for ext in (item.strip() for item in extra_exts_raw.split(',')) if ext]
all_extensions = normalize_ext_values(selected_exts + extra_exts)

button_col1, button_col2, button_col3, button_col4 = st.sidebar.columns(4)
start_clicked = button_col1.button('Start', use_container_width=True)
pause_clicked = button_col2.button('Pause', use_container_width=True)
resume_clicked = button_col3.button('Resume', use_container_width=True)
stop_clicked = button_col4.button('Stop', use_container_width=True)

include_patterns: List[str] = [line.strip() for line in include_patterns_raw.splitlines() if line.strip()]
exclude_patterns: List[str] = [line.strip() for line in exclude_patterns_raw.splitlines() if line.strip()]

if start_clicked:
    if not start_url:
        st.sidebar.error('Start URL is required.')
    elif not all_extensions:
        st.sidebar.error('Select at least one file extension to download.')
    else:
        try:
            crawler = FileCrawler(
                start_url=start_url,
                max_depth=max_depth,
                settings=settings,
                concurrency=concurrency,
                rate_limit=rate_limit,
                include_patterns=include_patterns,
                exclude_patterns=exclude_patterns,
                respect_robots=respect_robots,
                same_domain_only=same_domain,
                resume=resume_existing,
                download_concurrency=download_concurrency,
                target_extensions=all_extensions,
            )
        except ValueError as exc:
            st.sidebar.error(str(exc))
        else:
            st.session_state.crawler = crawler
            st.session_state.crawler.start()
            st.rerun()

crawler = st.session_state.crawler

if pause_clicked and crawler:
    crawler.pause()

if resume_clicked:
    if crawler:
        crawler.resume()
    elif resume_existing and start_url:
        if not all_extensions:
            st.sidebar.error('Select at least one file extension to download.')
        else:
            try:
                crawler = FileCrawler(
                    start_url=start_url,
                    max_depth=max_depth,
                    settings=settings,
                    concurrency=concurrency,
                    rate_limit=rate_limit,
                    include_patterns=include_patterns,
                    exclude_patterns=exclude_patterns,
                    respect_robots=respect_robots,
                    same_domain_only=same_domain,
                    resume=True,
                    download_concurrency=download_concurrency,
                    target_extensions=all_extensions,
                )
            except ValueError as exc:
                st.sidebar.error(str(exc))
            else:
                st.session_state.crawler = crawler
                st.session_state.crawler.start()
                st.rerun()

crawler = st.session_state.crawler

if stop_clicked and crawler:
    crawler.stop()
    st.session_state.crawler = None
    st.rerun()

status = crawler.get_status() if crawler else {}
st.session_state.status = status

st.title('Website File Scraper')
if status.get('target_extensions'):
    tracked = ', '.join(status['target_extensions'])
    st.caption(f'Tracking extensions: {tracked}')

progress_col, metrics_col = st.columns([2, 1])

with progress_col:
    pages = int(status.get('pages_visited', 0))
    frontier = int(status.get('frontier_size', 0))
    total = max(pages + frontier, 1)
    progress = pages / total
    st.progress(progress, text=f'Pages visited: {pages} | Queue size: {frontier}')

with metrics_col:
    st.metric('Files downloaded', status.get('asset_count', 0))
    st.metric('Unique files tracked', status.get('unique_assets', status.get('asset_count', 0)))
    st.metric('Downloads in progress', status.get('downloads_inflight', 0))
    st.metric('Crawler status', status.get('status', 'idle'))

st.subheader('Recent activity')
recent_events = status.get('events', [])
if recent_events:
    table_data = [
        {
            'Time': time.strftime('%H:%M:%S', time.localtime(event['timestamp'])),
            'Action': event['action'],
            'URL': event['url'],
            'Detail': event['detail'],
        }
        for event in recent_events
    ]
    st.dataframe(table_data, use_container_width=True)
else:
    st.write('No activity recorded yet.')

log_path = settings.logs_dir / 'scraper.log'
if log_path.exists():
    st.subheader('Log tail')
    tail_lines = log_path.read_text(encoding='utf-8', errors='ignore').splitlines()[-20:]
    st.code('\n'.join(tail_lines), language='text')

st.subheader('State and Reports')
state_file = settings.state_path
report_file = settings.report_path
manifest_file = settings.manifest_path
links_report_file = settings.links_report_path

if state_file.exists():
    st.write(f'Crawl state: {state_file}')
else:
    st.write('Crawl state: not yet created.')

if report_file.exists():
    st.write(f'Report: {report_file}')
else:
    st.write('Report: not yet created.')

if manifest_file.exists():
    st.write(f'Asset manifest: {manifest_file}')
else:
    st.write('Asset manifest: not yet created.')

if links_report_file.exists():
    st.write(f'Link depth report: {links_report_file}')
else:
    st.write('Link depth report: not yet created.')

st.markdown('---')
st.markdown(f'**Output directory:** `{settings.output_dir}`')
st.markdown(f'**Logs:** `{log_path}`')

if crawler and crawler.is_running():
    time.sleep(1.0)
    st.rerun()
