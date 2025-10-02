import json
import http.server
import socket
import threading
from pathlib import Path

import pytest

from scraper.config import ScraperSettings
from scraper.crawler import FileCrawler


@pytest.fixture()
def temp_server(tmp_path):
    (tmp_path / 'docs').mkdir()
    (tmp_path / 'docs' / 'files').mkdir(parents=True)

    index_html = tmp_path / 'index.html'
    index_html.write_text('<html><body><a href="docs/page.html">Docs</a></body></html>', encoding='utf-8')

    page_html = tmp_path / 'docs' / 'page.html'
    page_html.write_text(
        '<html><head><title>Test Files</title></head><body><a href="files/sample.pdf">Sample</a>'
        '<a href="page2.html">More</a></body></html>',
        encoding='utf-8',
    )

    page2_html = tmp_path / 'docs' / 'page2.html'
    page2_html.write_text(
        '<html><head><title>Second Page</title></head><body><a href="files/sample.pdf">Same</a></body></html>',
        encoding='utf-8',
    )

    pdf_path = tmp_path / 'docs' / 'files' / 'sample.pdf'
    pdf_path.write_bytes(b'%PDF-1.4\n%\xe2\xe3\xcf\xd3\n1 0 obj\n<<>>\nendobj\ntrailer\n<<>>\n%%EOF')

    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, format: str, *args) -> None:  # noqa: A003
            pass

    def serve(server):
        with server:
            server.serve_forever()

    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    host, port = sock.getsockname()
    sock.close()

    handler = lambda *args, **kwargs: QuietHandler(*args, directory=tmp_path, **kwargs)  # noqa: E731
    httpd = http.server.ThreadingHTTPServer(('127.0.0.1', port), handler)
    thread = threading.Thread(target=serve, args=(httpd,), daemon=True)
    thread.start()

    try:
        yield f'http://127.0.0.1:{port}/index.html'
    finally:
        httpd.shutdown()
        thread.join(timeout=2)


def test_crawler_downloads_file_and_reuses_cache(tmp_path, temp_server):
    settings = ScraperSettings(
        output_dir=tmp_path / 'out',
        logs_dir=tmp_path / 'logs',
        state_path=tmp_path / 'state.json',
        report_path=tmp_path / 'report.json',
        target_extensions=('.pdf',),
    )
    settings.ensure_directories()

    crawler = FileCrawler(
        start_url=temp_server,
        max_depth=2,
        settings=settings,
        concurrency=4,
        rate_limit=0.0,
        respect_robots=False,
        same_domain_only=True,
        target_extensions=['.pdf'],
        download_concurrency=2,
    )

    crawler.start()
    crawler.await_completion(timeout=10)
    crawler.stop()

    report = settings.report_path.read_text(encoding='utf-8')
    assert 'asset_count' in report

    downloaded = list((settings.output_dir).rglob('sample.pdf'))
    assert len(downloaded) == 1, 'PDF should be downloaded exactly once'
    downloaded_path = downloaded[0].resolve()

    asset_cache_paths = {Path(entry['path']).resolve() for entry in crawler.state.asset_cache.values()}
    assert asset_cache_paths == {downloaded_path}

    all_asset_paths = {
        Path(asset['path']).resolve()
        for record in crawler.state.pages.values()
        for asset in record.assets
    }
    assert all_asset_paths == {downloaded_path}
    page2_url = temp_server.replace('index.html', 'docs/page2.html')
    page2_record = crawler.state.pages.get(page2_url)
    assert page2_record is not None
    assert page2_record.assets, 'Cached asset should be recorded for second page'
    assert any(asset.get('reused') for asset in page2_record.assets)
    assert all(Path(asset['path']).resolve() == downloaded_path for asset in page2_record.assets)

    assert settings.manifest_path.exists()
    manifest_data = json.loads(settings.manifest_path.read_text(encoding='utf-8'))
    asset_url = next(iter(crawler.state.asset_manifest))
    manifest_entry = manifest_data['assets'][asset_url]
    assert manifest_entry['download_count'] == len(manifest_entry['pages']) == 2
    parent_url = temp_server.replace('index.html', 'docs/page.html')
    assert manifest_entry['first_page'] == parent_url
    assert any(page_info['page'] == page2_url and page_info.get('reused') for page_info in manifest_entry['pages'])

    assert settings.links_report_path.exists()
    links_report = json.loads(settings.links_report_path.read_text(encoding='utf-8'))
    level_map = {level['depth']: level['pages'] for level in links_report.get('levels', [])}
    assert any(page['url'] == page2_url and parent_url in page.get('referrers', []) for page in level_map.get(2, []))


    assert any(event.action == 'asset_reuse' for event in crawler._recent_events)
