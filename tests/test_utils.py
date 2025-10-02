from pathlib import Path

import pytest

from scraper import utils


def test_normalize_url_removes_fragment():
    base = 'https://example.com/path/index.html'
    href = '../docs/file.pdf#section'
    assert utils.normalize_url(base, href) == 'https://example.com/docs/file.pdf'


def test_normalize_url_filters_non_http():
    base = 'https://example.com'
    assert utils.normalize_url(base, 'mailto:test@example.com') is None


def test_same_registrable_domain():
    assert utils.same_registrable_domain('https://sub.example.com/a', 'https://example.com')
    assert not utils.same_registrable_domain('https://example.org', 'https://example.com')


def test_build_page_folder_with_title(tmp_path: Path):
    folder = utils.build_page_folder(tmp_path, 'https://example.com/docs/page', 'My PDF List')
    assert folder.relative_to(tmp_path).parts[0] == 'example.com'
    assert folder.name.startswith('my-pdf-list')


def test_filename_from_url_fallback():
    result = utils.filename_from_url('https://example.com/download', 'fallback.pdf')
    assert 'fallback' in result
