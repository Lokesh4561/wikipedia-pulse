from datetime import UTC, datetime

from ingest.fetch_pageviews import file_name, hours_to_fetch, source_url, volume_path

HOUR = datetime(2026, 11, 3, 14, tzinfo=UTC)


def test_file_name_and_paths():
    assert file_name(HOUR) == "pageviews-20261103-140000.gz"
    assert source_url(HOUR) == (
        "https://dumps.wikimedia.org/other/pageviews/2026/2026-11/pageviews-20261103-140000.gz"
    )
    assert volume_path(HOUR) == (
        "/Volumes/workspace/wiki_pulse/landing/pageviews/date=2026-11-03/pageviews-20261103-140000.gz"
    )


def test_hours_to_fetch_skips_unpublished_hours_and_is_oldest_first():
    now = datetime(2026, 11, 3, 16, 37, tzinfo=UTC)
    hours = hours_to_fetch(now, 3)
    assert [h.hour for h in hours] == [12, 13, 14]


def test_hours_to_fetch_crosses_midnight():
    now = datetime(2026, 11, 4, 1, 5, tzinfo=UTC)
    hours = hours_to_fetch(now, 2)
    assert [(h.day, h.hour) for h in hours] == [(3, 22), (3, 23)]
