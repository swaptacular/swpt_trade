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
    assert bid.peg_ptr == NULL
    assert bid.peg_exchange_rate == 1.0
    assert not bid.processed()
    assert not bid.deadend()
    assert not bid.anchor()
    
    bid.set_processed()
    assert bid.processed()
    bid.set_deadend()
    bid.set_deadend()
    assert bid.deadend()
    bid.set_anchor()
    bid.set_anchor()
    bid.set_anchor()
    assert bid.anchor()
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
        assert not bid.processed()
        assert not bid.deadend()
        assert not bid.anchor()
        bid.set_processed()
        bid.set_deadend()
        bid.set_anchor()
        assert bid.processed()
        assert bid.deadend()
        assert bid.anchor()
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
