# distutils: language = c++

import pytest
from . import cytest
from swpt_trade.pricing cimport Bid, BidRegistry


@cytest
def test_bid():
    cdef Bid* bid = new Bid(1, 101, -5000, 0, 1.0)
    assert bid != NULL
    assert bid.creditor_id == 1
    assert bid.debtor_id == 101
    assert bid.amount == -5000
    assert bid.anchor_id == 0
    assert bid.peg_ptr == NULL
    assert bid.peg_exchange_rate == 1.0
    assert not bid.priceable()
    assert not bid.visited()
    assert not bid.tradable()
    
    bid.set_visited()
    assert not bid.priceable()
    assert bid.visited()
    assert not bid.tradable()
    bid.set_tradable()
    assert not bid.priceable()
    assert bid.visited()
    assert bid.tradable()
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

    debtor_ids = []
    while (bid := r.get_priceable_bid()) != NULL:
        assert bid.priceable()
        assert not bid.visited()
        assert not bid.tradable()
        assert bid.anchor_id != 12345
        bid.set_visited()
        bid.set_tradable()
        bid.anchor_id = 12345
        assert bid.visited()
        assert bid.tradable()
        assert bid.anchor_id == 12345
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
