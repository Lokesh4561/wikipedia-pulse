from datetime import UTC, datetime

from ingest.fetch_export import check_export

NOW = datetime(2026, 11, 5, 6, 0, tzinfo=UTC)
GOOD = {
    "generated_at": "2026-11-05T05:55:00Z",
    "latest_hour": "2026-11-05T04:00:00Z",
    "languages": [{"code": "en"}],
    "trending": {},
    "top": {},
    "stats": {},
}


def test_fresh_export_has_no_problems():
    assert check_export(GOOD, NOW) == []


def test_stale_and_incomplete_exports_are_flagged():
    stale = {**GOOD, "generated_at": "2026-11-04T20:00:00Z"}
    assert any("stale" in p for p in check_export(stale, NOW))
    assert "missing key: stats" in check_export({k: v for k, v in GOOD.items() if k != "stats"}, NOW)
