import pytest
from datetime import timedelta, datetime, timezone
from swpt_trade.utils import parse_timedelta, can_start_new_turn


def test_parse_timedelta():
    assert parse_timedelta("3w") == timedelta(weeks=3)
    assert parse_timedelta("33d") == timedelta(days=33)
    assert parse_timedelta("123h") == timedelta(hours=123)
    assert parse_timedelta("1234m") == timedelta(minutes=1234)
    assert parse_timedelta("1000s") == timedelta(seconds=1000)
    assert parse_timedelta("1000s ") == timedelta(seconds=1000)
    assert parse_timedelta("1000s\n") == timedelta(seconds=1000)
    assert parse_timedelta("1000") == timedelta(seconds=1000)
    assert parse_timedelta("1000 \n") == timedelta(seconds=1000)

    with pytest.raises(ValueError):
        parse_timedelta("3x")
    with pytest.raises(ValueError):
        parse_timedelta("?s")
    with pytest.raises(ValueError):
        parse_timedelta("0s")
    with pytest.raises(ValueError):
        parse_timedelta(" 1s")


def test_can_start_new_turn():
    t = datetime(2025, 1, 1, 2, tzinfo=timezone.utc)
    h = timedelta(hours=1)

    def f(x, y=0):
        return can_start_new_turn(
            turn_period=timedelta(days=1),
            turn_period_offset=timedelta(hours=2),
            latest_turn_started_at=t + y * h,
            current_ts=t + x * h,
        )

    # Can not start a turn because the latest turn was too soon.
    assert not f(0)
    assert f(0, -1000)
    assert not f(11)
    assert f(11, -2)

    # Can not start a turn in the second half of the period.
    assert not f(12)
    assert not f(12, -1000)
    assert not f(13)
    assert not f(13, -1000)
    assert not f(23)
    assert not f(23, -1000)

    # Can start a turn in the first half of the period.
    assert f(24)
    assert f(24, 11)
    assert f(35)
    assert f(35, 22)

    # Can not start a turn in the second half of the period.
    assert not f(36)
    assert not f(36, -1000)
