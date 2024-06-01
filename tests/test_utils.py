import pytest
import math
from datetime import timedelta, datetime, timezone
from flask import current_app
from swpt_trade.utils import (
    SECONDS_IN_DAY,
    SECONDS_IN_YEAR,
    TT_BUYER,
    TT_COLLECTOR,
    TT_SELLER,
    parse_timedelta,
    can_start_new_turn,
    batched,
    calc_hash,
    i16_to_u16,
    u16_to_i16,
    i32_to_u32,
    u32_to_i32,
    contain_principal_overflow,
    calc_k,
    calc_demurrage,
    parse_transfer_note,
    generate_transfer_note,
    DispatchingData,
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
    assert list(batched('', 3)) == []
    assert list(batched('ABCDEFG', 3)) == [
        tuple("ABC"),
        tuple("DEF"),
        tuple("G"),
    ]
    assert list(batched('', 3)) == []

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


def test_i32_to_u32():
    assert i32_to_u32(-0x80000000) == 0x80000000
    assert i32_to_u32(-0x7fffffff) == 0x80000001
    assert i32_to_u32(-1) == 0xffffffff
    assert i32_to_u32(1) == 0x00000001
    assert i32_to_u32(0x7fffffff) == 0x7fffffff

    with pytest.raises(ValueError):
        i32_to_u32(0x80000000)
    with pytest.raises(ValueError):
        i32_to_u32(-0x80000001)


def test_u32_to_i32():
    assert u32_to_i32(0x80000000) == -0x80000000
    assert u32_to_i32(0x80000001) == -0x7fffffff
    assert u32_to_i32(0xffffffff) == -1
    assert u32_to_i32(0x00000001) == 1
    assert u32_to_i32(0x7fffffff) == 0x7fffffff

    with pytest.raises(ValueError):
        u32_to_i32(-1)
    with pytest.raises(ValueError):
        u32_to_i32(0x100000000)


def test_contain_principal_overflow():
    assert contain_principal_overflow(0) == 0
    assert contain_principal_overflow(1) == 1
    assert contain_principal_overflow(-1) == -1
    assert contain_principal_overflow(-1 << 63 + 1) == (-1 << 63) + 1
    assert contain_principal_overflow(-1 << 63) == (-1 << 63) + 1
    assert contain_principal_overflow((1 << 63) - 1) == (1 << 63) - 1
    assert contain_principal_overflow(1 << 63) == (1 << 63) - 1


def test_calc_k():
    eps = 1e-15
    assert SECONDS_IN_DAY == 24 * 3600
    assert 365 * SECONDS_IN_DAY < SECONDS_IN_YEAR < 366 * SECONDS_IN_DAY
    assert abs(math.exp(calc_k(0.0) * SECONDS_IN_YEAR) - 1.0) < eps
    assert abs(math.exp(calc_k(50.0) * SECONDS_IN_YEAR) - 1.5) < eps
    assert abs(math.exp(calc_k(80.0) * SECONDS_IN_YEAR) - 1.8) < eps
    assert abs(math.exp(calc_k(-50.0) * SECONDS_IN_YEAR) - 0.5) < eps
    assert abs(math.exp(calc_k(-80.0) * SECONDS_IN_YEAR) - 0.2) < eps


def test_calc_demurrage():
    assert 0.94 < calc_demurrage(-50, timedelta(days=30)) < 0.95
    assert 0.89 < calc_demurrage(-50, timedelta(days=60)) < 0.90
    assert calc_demurrage(-50, timedelta(days=0)) == 1.0
    assert calc_demurrage(50, timedelta(days=30)) == 1.0
    assert calc_demurrage(-50, timedelta(days=-30)) == 1.0
    assert calc_demurrage(-99.99999999999, timedelta(days=-30)) == 0.0
    assert calc_demurrage(-100, timedelta(days=-30)) == 0.0


@pytest.mark.parametrize("turn_id", [0, 1, -1, 2147483647, -2147483648])
@pytest.mark.parametrize("trader_type", [TT_BUYER, TT_COLLECTOR, TT_SELLER])
@pytest.mark.parametrize(
    "trader_id", [0, 1, -1, 9223372036854775807, -9223372036854775808]
)
def test_generate_and_parse_transfer_note(trader_type, turn_id, trader_id):
    s = generate_transfer_note(turn_id, trader_type, trader_id)
    assert parse_transfer_note(s) == (turn_id, trader_type, trader_id)


def test_generate_transfer_note_failure():
    for params in [
            (2147483648, TT_BUYER, 0),
            (-2147483649, TT_BUYER, 0),
            (0, TT_BUYER, 9223372036854775808),
            (0, TT_BUYER, -9223372036854775809),
            (0, "INVALID", 0),
    ]:
        with pytest.raises(ValueError):
            generate_transfer_note(*params)


def test_transfer_note_max_length(app):
    tt = sorted([TT_BUYER, TT_COLLECTOR, TT_SELLER], key=lambda x: len(x))[-1]
    s = generate_transfer_note(-1, tt, -1)
    min_bytes = current_app.config["APP_MIN_TRANSFER_NOTE_MAX_BYTES"]
    assert len(s.encode('utf-8')) <= min_bytes


def test_parse_transfer_note():
    assert (
        parse_transfer_note("Trading session: 1\nBuyer: b\n")
        == (1, "Buyer", 11)
    )
    assert (
        parse_transfer_note("Trading session: 4294967295\r\nBuyer: B\r\n")
        == (-1, "Buyer", 11)
    )
    assert (
        parse_transfer_note("Trading session: 0\r\nBuyer: Ffffffffffffffff")
        == (0, "Buyer", -1)
    )
    with pytest.raises(ValueError):
        parse_transfer_note("")
    with pytest.raises(ValueError):
        parse_transfer_note("Trading session: 0\nBuyer: 0Ffffffffffffffff\n")
    with pytest.raises(ValueError):
        parse_transfer_note("Trading session: 0\nBuyer: -0fffffffffffffff\n")
    with pytest.raises(ValueError):
        parse_transfer_note("Trading session: -1\nBuyer: f\n")
    with pytest.raises(ValueError):
        parse_transfer_note("Trading session: 1\nINVALID: 1\n")


def test_dispatching_data():
    dd = DispatchingData(2)
    dd.register_collecting(1, 2, 3, 100)
    dd.register_collecting(1, 2, 3, 150)
    dd.register_collecting(1, 2, 4, 300)
    dd.register_sending(1, 2, 3, 500)
    dd.register_receiving(1, 2, 3, 1000)
    dd.register_dispatching(1, 2, 3, 2000)

    ll = list(dd.statuses_iter())
    ll.sort(key=lambda x: (x["collector_id"], x["turn_id"], x["debtor_id"]))
    assert len(ll) == 2
    assert ll[0]["collector_id"] == 1
    assert ll[0]["turn_id"] == 2
    assert ll[0]["debtor_id"] == 3
    assert ll[0]["amount_to_collect"] == 250
    assert ll[0]["amount_to_send"] == 500
    assert ll[0]["number_to_receive"] == 1
    assert ll[0]["amount_to_dispatch"] == 2000
    assert ll[1]["collector_id"] == 1
    assert ll[1]["turn_id"] == 2
    assert ll[1]["debtor_id"] == 4
    assert ll[1]["amount_to_collect"] == 300
    assert ll[1]["amount_to_send"] == 0
    assert ll[1]["number_to_receive"] == 0
    assert ll[1]["amount_to_dispatch"] == 0
