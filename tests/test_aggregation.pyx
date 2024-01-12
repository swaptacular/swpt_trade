# distutils: language = c++

import pytest
from . import cytest
from swpt_trade.aggregation cimport (
    CollectorAccount,
    Solver,
)

@cytest
def test_collector_account_calc_hash():
    seen_values = set()
    for i in range(100):
        obj = CollectorAccount(i, i)
        h = obj.calc_hash()
        assert h not in seen_values

    h0 = CollectorAccount(0, 1).calc_hash()
    for i in range(100):
        obj = CollectorAccount(i, 1)
        h = obj.calc_hash()
        print(h)
        print(h0)
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
