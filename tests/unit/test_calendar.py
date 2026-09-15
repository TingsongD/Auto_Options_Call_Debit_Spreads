"""M1-02 calendar, session math and review grid (T07)."""

from datetime import UTC, date, datetime, timedelta

import pytest

from spx_research.temporal.calendar import (
    CalendarError,
    CalendarManifest,
    build_weekday_manifest,
    load_manifest,
)


def cal() -> CalendarManifest:
    return build_weekday_manifest(
        "test-cal",
        date(2019, 1, 1),
        date(2019, 3, 29),
        holidays=frozenset({date(2019, 1, 21)}),  # MLK
        half_days=frozenset({date(2019, 3, 8)}),
    )


def test_normal_day_review_grid():
    reviews = cal().review_times(date(2019, 1, 2), 15)
    assert len(reviews) == 25  # 09:45 .. 15:45 ET
    assert reviews[0] == datetime(2019, 1, 2, 14, 45, tzinfo=UTC)  # 09:45 ET
    assert reviews[-1] == datetime(2019, 1, 2, 20, 45, tzinfo=UTC)  # 15:45 ET
    s = cal().session(date(2019, 1, 2))
    assert s is not None and reviews[-1] < s.close_utc()


def test_half_day_grid_and_close():
    reviews = cal().review_times(date(2019, 3, 8), 15)
    s = cal().session(date(2019, 3, 8))
    assert s is not None and s.half_day and s.close_utc() == datetime(2019, 3, 8, 18, 0, tzinfo=UTC)
    assert len(reviews) == 13  # last review 12:45 ET, none at the 13:00 close
    assert all(r < s.close_utc() for r in reviews)


def test_holiday_and_weekend_not_sessions():
    assert not cal().is_session(date(2019, 1, 21))
    assert not cal().is_session(date(2019, 1, 5))  # Saturday
    assert cal().review_times(date(2019, 1, 21), 15) == []


def test_dst_boundary_uses_ny_zoneinfo():  # T07
    # March 10 2019 spring-forward: 09:30 ET is 14:30 UTC, not 13:30.
    c = build_weekday_manifest("dst", date(2019, 3, 11), date(2019, 3, 11))
    s = c.session(date(2019, 3, 11))
    assert s is not None
    assert s.open_utc() == datetime(2019, 3, 11, 13, 30, tzinfo=UTC)
    pre = build_weekday_manifest("dst", date(2019, 3, 8), date(2019, 3, 8))
    s0 = pre.session(date(2019, 3, 8))
    assert s0 is not None
    assert s0.open_utc() == datetime(2019, 3, 8, 14, 30, tzinfo=UTC)


def test_minute_offsets_and_utc_minute():
    s = cal().session(date(2019, 1, 2))
    assert s is not None
    assert list(s.minute_offsets())[:3] == [0, 1, 2]
    assert len(list(s.minute_offsets())) == 390
    assert cal().utc_minute(date(2019, 1, 2), 0) == s.open_utc()
    assert cal().utc_minute(date(2019, 1, 2), 389) == s.open_utc() + timedelta(minutes=389)
    with pytest.raises(CalendarError):
        cal().utc_minute(date(2019, 1, 2), 390)
    with pytest.raises(CalendarError):
        cal().utc_minute(date(2019, 1, 21), 0)


def test_manifest_integrity(tmp_path):
    m = tmp_path / "cal.json"
    m.write_text(
        '{"calendar_id": "x", "version": "1", "sessions": '
        '[{"date": "2019-01-03"}, {"date": "2019-01-02"}]}'
    )
    with pytest.raises(CalendarError):
        load_manifest(m)  # unsorted
    m.write_text(
        '{"calendar_id": "x", "version": "1", "sessions": '
        '[{"date": "2019-01-02"}, {"date": "2019-01-02"}]}'
    )
    with pytest.raises(CalendarError):
        load_manifest(m)  # duplicate
