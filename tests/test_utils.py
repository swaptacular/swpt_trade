import pytest
from datetime import timedelta, datetime, timezone
from swpt_trade.utils import (
    parse_timedelta,
    can_start_new_turn,
    batched,
    calc_hash,
    i16_to_u16,
    u16_to_i16,
    calc_iri_routing_key,
)


def test_parse_timedelta():
    assert parse_timedelta("3w") == timedelta(weeks=3)
    assert parse_timedelta("33d") == timedelta(days=33)
    assert parse_timedelta("123h") == timedelta(hours=123)
    assert parse_timedelta("0.5h") == timedelta(minutes=30)
    assert parse_timedelta(".5h") == timedelta(minutes=30)
    assert parse_timedelta("30.0m") == timedelta(minutes=30)
    assert parse_timedelta("5e-1h") == timedelta(minutes=30)
    assert parse_timedelta("0.05e1h") == timedelta(minutes=30)
    assert parse_timedelta("0.05e+1h") == timedelta(minutes=30)
    assert parse_timedelta("1234m") == timedelta(minutes=1234)
    assert parse_timedelta("1000s") == timedelta(seconds=1000)
    assert parse_timedelta("1000s ") == timedelta(seconds=1000)
    assert parse_timedelta("1000s\n") == timedelta(seconds=1000)
    assert parse_timedelta("1000") == timedelta(seconds=1000)
    assert parse_timedelta("1000 \n") == timedelta(seconds=1000)
    assert parse_timedelta("0") == timedelta(seconds=0)

    with pytest.raises(ValueError):
        parse_timedelta("1.2.3")
    with pytest.raises(ValueError):
        parse_timedelta("3x")
    with pytest.raises(ValueError):
        parse_timedelta("?s")
    with pytest.raises(ValueError):
        parse_timedelta("-5s")
    with pytest.raises(ValueError):
        parse_timedelta(" 1s")


def test_calc_iri_routing_key():
    h1 = calc_iri_routing_key("https://example.com/iri")
    h2 = calc_iri_routing_key("https://example.com/iri2")
    assert h1 == '0.1.0.0.0.0.1.1.1.1.0.1.0.1.0.0.1.1.0.0.0.1.0.1'
    assert h2 == '0.0.0.1.0.1.0.1.0.0.0.1.1.1.1.1.0.0.1.1.1.0.1.0'


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

    # Can never start when the period is zero.
    assert not can_start_new_turn(
        turn_period=timedelta(seconds=0),
        turn_period_offset=timedelta(seconds=0),
        latest_turn_started_at=t - 1000 * h,
        current_ts=t,
    )


def test_batched():
    assert list(batched('ABCDEFG', 3)) == [
        tuple("ABC"),
        tuple("DEF"),
        tuple("G"),
    ]
    with pytest.raises(ValueError):
        list(batched('ABCDEFG', 0))


def test_calc_hash():
    assert calc_hash(123) == u16_to_i16(0b1111110000010000)


def test_i16_to_u16():
    assert i16_to_u16(-0x8000) == 0x8000
    assert i16_to_u16(-0x7fff) == 0x8001
    assert i16_to_u16(-1) == 0xffff
    assert i16_to_u16(1) == 0x0001
    assert i16_to_u16(0x7fff) == 0x7fff

    with pytest.raises(ValueError):
        i16_to_u16(0x8000)
    with pytest.raises(ValueError):
        i16_to_u16(-0x8001)


def test_u16_to_i16():
    assert u16_to_i16(0x8000) == -0x8000
    assert u16_to_i16(0x8001) == -0x7fff
    assert u16_to_i16(0xffff) == -1
    assert u16_to_i16(0x0001) == 1
    assert u16_to_i16(0x7fff) == 0x7fff

    with pytest.raises(ValueError):
        u16_to_i16(-1)
    with pytest.raises(ValueError):
        u16_to_i16(0x10000)
