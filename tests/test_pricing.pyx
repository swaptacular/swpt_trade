# distutils: language = c++

import pytest
import math
from . import cytest
from swpt_trade.pricing cimport (
    compare_prices,
    Key128,
    Peg,
    PegRegistry,
    Bid,
    BidRegistry,
    BidProcessor,
)


@cytest
def test_compare_prices():
    assert compare_prices(1.0, 1.0)
    assert compare_prices(-1.0, -1.0)
    assert compare_prices(1.0, 1.000001)
    assert compare_prices(-1.0, -1.000001)
    assert compare_prices(1e-25, 1.000001e-25)
    assert compare_prices(1e+25, 1.000001e+25)
    assert compare_prices(-1e-25, -1.000001e-25)
    assert compare_prices(-1e+25, -1.000001e+25)

    assert not compare_prices(0.0, 0.000001)
    assert not compare_prices(0.0, 0.0)
    assert not compare_prices(1.0, 0.0)
    assert not compare_prices(0.0, 1.0)
    assert not compare_prices(-1.0, 0.0)
    assert not compare_prices(0.0, -1.0)
    assert not compare_prices(-1.0, 1.0)
    assert not compare_prices(1.0, -1.0)
    assert not compare_prices(1.0, 1.000011)
    assert not compare_prices(1e-25, 1.000011e-25)
    assert not compare_prices(1e+25, 1.000011e+25)
    assert not compare_prices(-1e-25, -1.000011e-25)
    assert not compare_prices(-1e+25, -1.000011e+25)
    assert not compare_prices(1.0, math.inf)
    assert not compare_prices(1.0, -math.inf)
    assert not compare_prices(1.0, math.nan)
    assert not compare_prices(math.inf, 1.0)
    assert not compare_prices(math.inf, math.inf)
    assert not compare_prices(math.inf, -math.inf)
    assert not compare_prices(math.inf, math.nan)
    assert not compare_prices(-math.inf, 1.0)
    assert not compare_prices(-math.inf, math.inf)
    assert not compare_prices(-math.inf, -math.inf)
    assert not compare_prices(-math.inf, math.nan)
    assert not compare_prices(math.nan, 1.0)
    assert not compare_prices(math.nan, math.nan)
    assert not compare_prices(math.nan, math.inf)
    assert not compare_prices(math.nan, -math.inf)


@cytest
def test_bid():
    cdef Bid* bid = new Bid(1, 101, -5000, 0, 1.0)
    assert bid != NULL
    assert bid.creditor_id == 1
    assert bid.debtor_id == 101
    assert bid.amount == -5000
    assert bid.peg_ptr == NULL
    assert bid.peg_exchange_rate == 1.0
    
    del bid


@cytest
def test_bid_registry():
    cdef BidRegistry* r = new BidRegistry(101)
    assert r.base_debtor_id == 101
    r.add_bid(1, 0, 1000, 101, 1.0)  # ignored

    # priceable
    r.add_bid(1, 101, 6000, 0, 0.0)
    r.add_bid(1, 102, 5000, 101, 1.0)
    r.add_bid(1, 122, 5000, 101, 1.0)
    r.add_bid(1, 103, 4000, 102, 10.0)
    r.add_bid(1, 133, 4000, 102, 10.0)

    # not priceable
    r.add_bid(1, 104, 3000, 0, 1.0)
    r.add_bid(1, 105, 2000, 104, 1.0)
    r.add_bid(1, 155, 2000, 666, 1.0)

    # not priceable (a peg cylce)
    r.add_bid(1, 106, 900, 107, 1.0)
    r.add_bid(1, 107, 800, 106, 1.0)

    # not priceable (a different trader)
    r.add_bid(2, 123, 5000, 101, 1.0)

    with pytest.raises(RuntimeError):
        r.add_bid(2, 123, 5000, 101, 1.0)  # duplicated

    debtor_ids = []
    while (bid := r.get_priceable_bid()) != NULL:
        debtor_ids.append(bid.debtor_id)

    assert len(debtor_ids) == 5
    assert sorted(debtor_ids) == [101, 102, 103, 122, 133]

    with pytest.raises(RuntimeError):
        r.add_bid(1, 108, 700, 0, 1.0)

    del r

@cytest
def test_empty_bid_registry():
    cdef BidRegistry* r = new BidRegistry(101)
    assert r.get_priceable_bid() == NULL

    with pytest.raises(RuntimeError):
        r.add_bid(1, 108, 700, 0, 1.0)

    del r


@cytest
def test_key128_calc_hash():
    seen_values = set()
    for i in range(100):
        for j in range(100):
            k = Key128(i, j)
            h = k.calc_hash()
            assert h not in seen_values


@cytest
def test_peg():
    cdef Peg* p = new Peg(101, Key128(0, 0), 102, 2.0)
    assert p != NULL
    assert p.debtor_id == 101
    assert p.peg_exchange_rate == 2.0
    assert p.peg_ptr == NULL
    assert p.confirmed() is False
    assert p.tradable() is False
    del p


@cytest
def test_peg_registry():
    import math

    cdef PegRegistry* r = new PegRegistry(Key128(100, 1), 101, 2)
    assert r.base_debtor_key.first == 100
    assert r.base_debtor_key.second == 1
    assert r.base_debtor_id == 101
    assert r.max_distance_to_base == 2

    r.add_currency(Key128(100, 2), 102, Key128(100, 1), 101, 2.0, True)
    r.add_currency(Key128(100, 3), 103, Key128(100, 2), 102, 3.0, True)
    r.add_currency(Key128(100, 4), 104, Key128(100, 2), 102, 4.0, False)
    r.add_currency(Key128(100, 5), 105, Key128(100, 3), 103, 5.0, False)
    r.add_currency(Key128(100, 6), 106, Key128(100, 4), 104, 6.0, True)
    r.add_currency(Key128(100, 7), 107, Key128(100, 1), 101, 0.5, False)
    r.add_currency(Key128(100, 8), 108, Key128(100, 7), 107, 1.0, True)
    r.add_currency(Key128(100, 9), 109, Key128(100, 7), 107, 2.0, False)
    r.add_currency(Key128(100, 10), 110, Key128(100, 1), 0, 1.0, True)

    # ignored invalid debtor_id
    r.add_currency(Key128(100, 20), 0, Key128(100, 1), 101, 2.0, False)

    with pytest.raises(RuntimeError):
        # duplicated debtor key
        r.add_currency(Key128(100, 2), 102, Key128(100, 1), 101, 2.0, True)

    with pytest.raises(RuntimeError):
        # not prepared
        r.get_currency_price(101)

    with pytest.raises(RuntimeError):
        # not prepared
        r.get_tradable_currency(101)

    r.prepare_for_queries()

    with pytest.raises(RuntimeError):
        # after preparation
        r.add_currency(Key128(100, 11), 111, Key128(100, 7), 107, 1.0, False)

    for _ in range(2):
        assert math.isnan(r.get_currency_price(101))
        assert r.get_currency_price(102) == 2.0
        assert r.get_currency_price(103) == 2.0 * 3.0
        assert math.isnan(r.get_currency_price(104))
        assert math.isnan(r.get_currency_price(105))
        assert math.isnan(r.get_currency_price(106))
        assert math.isnan(r.get_currency_price(107))
        assert r.get_currency_price(108) == 0.5
        assert math.isnan(r.get_currency_price(109))
        assert math.isnan(r.get_currency_price(110))
        assert math.isnan(r.get_currency_price(666))

        assert r.get_tradable_currency(101) == NULL

        p102 = r.get_tradable_currency(102)
        assert p102.debtor_id == 102
        assert p102.peg_exchange_rate == 2.0
        assert p102.peg_ptr.debtor_id == 101
        assert p102.confirmed()
        assert p102.tradable()

        p103 = r.get_tradable_currency(103)
        assert p103.debtor_id == 103
        assert p103.peg_exchange_rate == 3.0
        assert p103.peg_ptr.debtor_id == 102
        assert p103.confirmed()
        assert p103.tradable()

        assert r.get_tradable_currency(104) == NULL
        assert r.get_tradable_currency(105) == NULL
        assert r.get_tradable_currency(106) == NULL
        assert r.get_tradable_currency(107) == NULL

        p108 = r.get_tradable_currency(108)
        assert p108.debtor_id == 108
        assert p108.peg_exchange_rate == 1.0
        assert p108.peg_ptr.debtor_id == 107
        assert p108.confirmed()
        assert p108.tradable()

        assert r.get_tradable_currency(109) == NULL
        assert r.get_tradable_currency(110) == NULL
        assert r.get_tradable_currency(666) == NULL

        r.prepare_for_queries()  # this should be possible

    del r


@cytest
def test_bp_calc_key128():
    import hashlib
    import sys

    bp = BidProcessor('', 1)
    for x in range(20):
        s = f'test{x}'
        key = bp._calc_key128(s)
        m = hashlib.sha256()
        m.update(s.encode('utf8'))
        digest = m.digest()
        first = int.from_bytes(digest[:8], sys.byteorder, signed="True")
        second = int.from_bytes(digest[8:16], sys.byteorder, signed="True")
        assert first == key.first
        assert second == key.second


@cytest
def test_bp_candidate_offers():
    bp = BidProcessor('https://x.com/101', 101, 2, 1000)
    bp.register_currency(False, 'https://x.com/101', 101)  # base
    bp.register_currency(
        True, 'https://x.com/102', 102, 'https://x.com/101', 101, 2.0
    )
    bp.register_currency(
        True, 'https://x.com/103', 103, 'https://x.com/102', 102, 3.0
    )
    bp.register_currency(
        True, 'https://x.com/104', 104, 'https://x.com/103', 103, 4.0
    )
    bp.register_currency(
        False, 'https://x.com/105', 105, 'https://x.com/101', 101, 5.0
    )
    bp.register_currency(
        True, 'https://x.com/106', 106, 'https://x.com/105', 105, 6.0
    )
    bp.register_currency(
        False, 'https://x.com/107', 107, 'https://x.com/102', 102, 7.0
    )
    bp.register_currency(
        True, 'https://x.com/108', 108, 'https://x.com/102', 666, 1.0
    )
    bp.register_currency(
        True, 'https://x.com/109', 109, 'https://x.com/666', 102, 1.0
    )
    assert math.isnan(bp.get_currency_price(101))
    assert bp.get_currency_price(102) == 2.0
    assert bp.get_currency_price(103) == 6.0
    assert math.isnan(bp.get_currency_price(104))
    assert math.isnan(bp.get_currency_price(107))
    assert math.isnan(bp.get_currency_price(108))
    assert math.isnan(bp.get_currency_price(109))
    assert math.isnan(bp.get_currency_price(105))
    assert bp.get_currency_price(106) == 30.0

    bp.register_bid(1, 101, 666666)  # not tradable
    bp.register_bid(1, 105, -666666, 101, 5.0)  # not tradable
    bp.register_bid(1, 106, -50000, 105, 6.000005)  # OK!
    bp.register_bid(1, 102, 10000, 101, 2.0)  # OK!
    bp.register_bid(1, 103, 100, 102, 3.0)  # abs(amount) is too small

    bp.register_bid(2, 101, 666666)  # not tradable
    bp.register_bid(2, 105, -666666, 101, 5.0)  # not tradable
    bp.register_bid(2, 106, -50000, 105, 6.0)  # OK, but no buyer for this!
    bp.register_bid(2, 102, 10000, 101, 1.999) # wrongly priced
    bp.register_bid(2, 103, 10000, 102, 3.0)  # pegged to wrongly priced

    bp.register_bid(3, 101, 666666)  # not tradable
    bp.register_bid(3, 105, -666666, 101, 5.0)  # not tradable
    bp.register_bid(3, 106, 50000, 105, 6.0)  # OK, but no seller for this!
    bp.register_bid(3, 102, -100, 101, 2.0)  # abs(amount) is too small
    bp.register_bid(3, 103, 10000, 102, 3.0)  # OK, but no seller for this!
    bp.register_bid(3, 104, -10000, 103, 4.0)  # too big distance to base

    bp.register_bid(4, 105, -666666, 101, 5.0)  # no base bid
    bp.register_bid(4, 106, -50000, 105, 6.000005)  # no base bid
    bp.register_bid(4, 102, 10000, 101, 2.0)  # no base bid
    bp.register_bid(4, 103, 100, 102, 3.0)  # no base bid

    offers = bp.generate_candidate_offers()
    assert len(offers) == 2

    offer0, offer1 = offers
    if offer0.debtor_id != 102:
        offer0, offer1 = offer1, offer0

    assert offer0.debtor_id == 102
    assert offer0.creditor_id == 1
    assert offer0.amount == 10000
    assert offer0.is_buy_offer()
    assert not offer0.is_sell_offer()

    assert offer1.debtor_id == 106
    assert offer1.creditor_id == 1
    assert offer1.amount == -50000
    assert not offer1.is_buy_offer()
    assert offer1.is_sell_offer()

    assert len(bp.generate_candidate_offers()) == 0
    assert len(bp.generate_candidate_offers()) == 0

    bp.register_bid(5, 101, 666666)  # not tradable
    bp.register_bid(5, 105, -666666, 101, 5.0)  # not tradable
    bp.register_bid(5, 106, -50000, 105, 6.000005)  # OK!
    bp.register_bid(5, 102, 10000, 101, 2.0)  # OK!
    bp.register_bid(5, 103, 10000, 102, 3.0)  # OK!
    assert len(bp.generate_candidate_offers()) == 3
    assert len(bp.generate_candidate_offers()) == 0

