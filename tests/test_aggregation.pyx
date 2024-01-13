# distutils: language = c++

import pytest
from . import cytest
from swpt_trade.aggregation cimport (
    Account,
    CollectorAccount,
    Solver,
)


@cytest
def test_account_calc_hash():
    seen_values = set()
    for i in range(0, 500000, 5000):
        for j in range(1000000, 1700000, 7000):
            account = Account(i, j)
            h = account.calc_hash()
            assert h not in seen_values
            seen_values.add(h)


@cytest
def test_collector_account_calc_hash():
    seen_values = set()
    for i in range(100):
        obj = CollectorAccount(i, i)
        h = obj.calc_hash()
        assert h not in seen_values
        seen_values.add(h)

    h0 = CollectorAccount(0, 1).calc_hash()
    for i in range(100):
        obj = CollectorAccount(i, 1)
        h = obj.calc_hash()
        assert h == h0


@cytest
def test_get_random_collector_id():
    s = Solver('https://example.com/base', 666)
    assert s._get_random_collector_id(123, 666) == 123

    s.register_collector_account(1, 666)
    s.register_collector_account(2, 666)
    s.register_collector_account(3, 666)
    s.register_collector_account(1, 777)

    all_ids = [1, 2, 3]
    seen_ids = set()
    for _ in range(10000):
        x = s._get_random_collector_id(123, 666)
        assert x in all_ids
        seen_ids.add(x)
        if all([x in seen_ids for x in all_ids]):
            break
    else:
        assert 0, "non-random collector ids"

    assert s._get_random_collector_id(123, 777) == 1


@cytest
def test_register_offers():
    s = Solver('https://example.com/101', 101)
    s.register_currency(True, 'https://example.com/101', 101)
    s.register_currency(
        True,
        'https://example.com/102', 102,
        'https://example.com/101', 101,
        2.0,
    )
    s.register_currency(
        True,
        'https://example.com/103', 103,
        'https://example.com/101', 101,
        0.5,
    )
    s.analyze_currencies()

    # creditor 1
    s.register_sell_offer(1, 101, 200000, 999)
    s.register_buy_offer(1, 102, 50000)
    s.register_buy_offer(1, 103, 50000)

    # creditor 2
    s.register_sell_offer(2, 102, 50000, 999)
    s.register_buy_offer(2, 101, 50000)

    # creditor 3
    s.register_sell_offer(3, 103, 50000, 999)
    s.register_buy_offer(3, 101, 50000)

    s.analyze_offers()

    takings = sorted(
        list(s.takings_iter()),
        key=lambda x: (x.debtor_id, x.creditor_id),
    )
    assert len(takings) == 3
    assert takings[0] == (1, 101, -75000, 999)
    assert takings[1] == (2, 102, -25000, 999)
    assert takings[2] == (3, 103, -50000, 999)

    givings = sorted(
        list(s.givings_iter()),
        key=lambda x: (x.debtor_id, x.creditor_id),
    )
    assert len(givings) == 4
    assert givings[0] == (2, 101, 50000, 999)
    assert givings[1] == (3, 101, 25000, 999)
    assert givings[2] == (1, 102, 25000, 999)
    assert givings[3] == (1, 103, 50000, 999)


@cytest
def test_self_trade():
    s = Solver('https://example.com/101', 101)
    s.register_currency(True, 'https://example.com/101', 101)
    s.analyze_currencies()
    s.register_sell_offer(1, 101, 10000, 999)
    s.register_buy_offer(1, 101, 20000)
    s.analyze_offers()

    assert s.collection_amounts.count(Account(999, 101)) == 1
    assert s.collection_amounts.at(Account(999, 101)) == 0
    assert len(list(s.takings_iter())) == 0
    assert len(list(s.givings_iter())) == 0
