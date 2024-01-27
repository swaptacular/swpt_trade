# distutils: language = c++

import pytest
import math
import array
from . import cytest
from swpt_trade.solver.matching cimport (
    Arc,
    Node,
    NodeRegistry,
    Digraph,
    INF_AMOUNT,
)

@cytest
def test_infinity():
    assert INF_AMOUNT == math.inf


@cytest
def test_arc():
    cdef Arc* arc = new Arc(<Node*>123, 50.0)
    assert arc != NULL
    assert arc.node_ptr == <Node*>123
    assert arc.amount == 50.0
    del arc


@cytest
def test_node():
    cdef Node* node = new Node(1, 100.0, 3)
    assert node != NULL
    assert node.id == 1
    assert node.min_amount == 100.0
    assert node.status == 3
    assert node.arcs_count() == 0

    with pytest.raises(IndexError):
        node.get_arc(0)

    cdef Arc* added_arc = &node.add_arc(node, 50.0)
    assert node.arcs_count() == 1
    assert added_arc.node_ptr == node
    assert added_arc.amount == 50.0

    cdef Arc* arc_ptr = &node.get_arc(0)
    assert node.arcs_count() == 1
    assert arc_ptr.node_ptr == node
    assert arc_ptr.amount == 50.0
    arc_ptr.node_ptr = NULL
    arc_ptr.amount = 49.0

    cdef Arc* updated_arc = &node.get_arc(0)
    assert node.arcs_count() == 1
    assert updated_arc.node_ptr == NULL
    assert updated_arc.amount == 49.0
    assert node.arcs_count() == 1

    with pytest.raises(IndexError):
        node.get_arc(1)

    del node


@cytest
def test_node_registry():
    cdef NodeRegistry nodes
    assert nodes.get_node(1) == NULL
    assert nodes.create_node(1, 100.0, 3) != NULL
    node_ptr = nodes.get_node(1)
    assert node_ptr != NULL
    assert node_ptr.id == 1
    assert node_ptr.min_amount == 100.0
    assert node_ptr.status == 3


@cytest
def test_digraph_construction():
    g = Digraph()
    assert g.path.size() == 1
    cdef Node* root = g.path.back()
    assert root != NULL
    assert root.arcs_count() == 0
    assert root.status == 0

    with pytest.raises(ValueError):
        g.add_supply(100.0, 666, 2)

    with pytest.raises(ValueError):
        g.add_demand(100.0, 666, 1)

    with pytest.raises(ValueError):
        g.get_min_amount(666)

    assert root.arcs_count() == 0
    g.add_currency(666, 100.0)
    assert root.arcs_count() == 1
    assert g.get_min_amount(666) == 100.0

    with pytest.raises(ValueError):
        g.add_currency(666, 100.0)

    g.add_supply(1000.0, 666, 1)
    g.add_supply(2000.0, 666, 2)
    g.add_demand(500.0, 666, 2)

    assert root.id == 0
    assert root.min_amount == 0.0
    assert root.arcs_count() == 1
    cdef Arc* arc = &root.get_arc(0)
    assert math.isinf(arc.amount)

    currency = arc.node_ptr
    assert currency.id == 666
    assert currency.min_amount == 100.0
    assert currency.arcs_count() == 2

    cdef Arc* a0 = &currency.get_arc(0)
    assert a0.node_ptr.id == 1
    assert a0.node_ptr.arcs_count() == 0
    assert a0.amount == 1000.0

    cdef Arc* a1 = &currency.get_arc(1)
    assert a1.node_ptr.id == 2
    assert a1.node_ptr.arcs_count() == 1
    assert a1.amount == 2000.0

    cdef Arc* trader1_arc = &a1.node_ptr.get_arc(0)
    assert trader1_arc.node_ptr.id == 666
    assert trader1_arc.amount == 500.0


@cytest
def test_digraph_value_errors():
    g = Digraph()
    huge_int = 1234567890123456789012345

    for params in [
        (huge_int, 100.0),
        (666, -1.0),
        (666, math.nan),
    ]:
        with pytest.raises((ValueError, OverflowError)):
            g.add_currency(params[0], params[1])

    g.add_currency(666, 100.0)

    for params in [
        (0.0, huge_int, 1),
        (0.0, 666, huge_int),
    ]:
        with pytest.raises((ValueError, OverflowError)):
            g.add_supply(params[0], params[1], params[2])

    for params in [
        (0.0, huge_int, 1),
        (0.0, 666, huge_int),
    ]:
        with pytest.raises((ValueError, OverflowError)):
            g.add_demand(params[0], params[1], params[2])


@cytest
def test_digraph_find_no_cylcles():
    g = Digraph()
    g.add_currency(666, 100.0)
    g.add_currency(999, 50.0)
    g.add_supply(1000.0, 666, 1)
    g.add_supply(2000.0, 999, 3)
    g.add_demand(500.0, 666, 2)
    g.add_demand(10.0, 666, 1)  # too small
    g.add_demand(250.0, 666, 3)
    assert not g._find_cycle()
    assert g.path.size() == 0

    with pytest.raises(RuntimeError):
        g.add_demand(5000.0, 999, 1)


@cytest
def test_digraph_process_cycle():
    g = Digraph()
    g.add_currency(666, 100.0)
    g.add_currency(999, 50.0)

    g.add_demand(1000.0, 666, 1)
    g.add_demand(2000.0, 999, 3)

    g.add_supply(500.0, 666, 2)
    g.add_supply(10.0, 666, 1)  # too small
    g.add_supply(250.0, 666, 3)
    g.add_supply(5000.0, 999, 1)

    assert g._find_cycle()
    assert g.path.size() == 5
    assert g.path.back().id == 1

    # the cycle:
    assert g.traders.get_node(1).status == 0b000
    assert g.currencies.get_node(999).status == 0b001
    assert g.traders.get_node(3).status == 0b001
    assert g.currencies.get_node(666).status == 0b101

    assert g.traders.get_node(2).status == 0b000

    amount, cycle = g._process_cycle()
    assert amount == 250.0
    assert list(cycle) == [666, 1, 999, 3]


@cytest
def test_digraph_simple_cylcle():
    g = Digraph()
    g.add_currency(666, 100.0)
    g.add_currency(999, 50.0)

    g.add_demand(1000.0, 666, 1)
    g.add_demand(2000.0, 999, 3)

    g.add_supply(500.0, 666, 2)
    g.add_supply(10.0, 666, 1)  # too small
    g.add_supply(250.0, 666, 3)
    g.add_supply(5000.0, 999, 1)

    amount, cycle = g.find_cycle()
    assert isinstance(amount, float)
    assert isinstance(cycle, array.array)
    assert amount == 250.0
    assert list(cycle) in [[666, 1, 999, 3], [999, 3, 666, 1]]

    assert g.find_cycle() is None
    assert g.find_cycle() is None


@cytest
def test_digraph_self_cylcle():
    g = Digraph()
    g.add_currency(666, 100.0)
    g.add_demand(1000.0, 666, 1)
    g.add_supply(2000.0, 666, 1)
    amount, cycle = g.find_cycle()
    assert amount == 1000.0
    assert list(cycle) == [666, 1]
    assert g.find_cycle() is None


@cytest
def test_digraph_overlapping_cylcles():
    g = Digraph()
    g.add_currency(101, 50.0)
    g.add_currency(102, 50.0)
    g.add_currency(103, 50.0)
    g.add_currency(104, 50.0)

    g.add_demand(150.0, 103, 1)
    g.add_demand(1000.0, 103, 2)
    g.add_demand(1000.0, 104, 2)
    g.add_demand(1000.0, 101, 3)
    g.add_demand(300.0, 102, 4)

    g.add_supply(1000.0, 101, 1)
    g.add_supply(50.0, 102, 2)
    g.add_supply(1000.0, 102, 3)
    g.add_supply(100.0, 101, 4)
    g.add_supply(200.0, 103, 4)

    deals = list(g.cycles())
    deals.sort(key=lambda t: t[0])
    assert len(deals) == 3
    assert [amt for amt, nodes in deals] == [50.0, 100.0, 150.0]
    assert [len(nodes) for amt, nodes in deals] == [4, 4, 6]

    # The cycle iterator has been exhausted.
    assert len(list(g.cycles())) == 0

    # Check sort ranks.
    assert g.traders.get_node(0).sort_rank == 5
    assert g.traders.get_node(1).sort_rank == 1
    assert g.traders.get_node(2).sort_rank == 1
    assert g.traders.get_node(3).sort_rank == 2
    assert g.traders.get_node(4).sort_rank == 2
    assert g.currencies.get_node(101).sort_rank == 2
    assert g.currencies.get_node(102).sort_rank == 2
    assert g.currencies.get_node(103).sort_rank == 1
    assert g.currencies.get_node(104).sort_rank == 0

    # Check the order of arcs.
    assert g.traders.get_node(0).get_arc(0).node_ptr.id in [101, 102]
    assert g.traders.get_node(0).get_arc(1).node_ptr.id in [101, 102]
    assert g.traders.get_node(0).get_arc(2).node_ptr.id == 103
    assert g.traders.get_node(0).get_arc(3).node_ptr.id == 104
    assert g.traders.get_node(1).get_arc(0).node_ptr.id == 103
    assert g.traders.get_node(2).get_arc(0).node_ptr.id == 103
    assert g.traders.get_node(2).get_arc(1).node_ptr.id == 104
    assert g.traders.get_node(3).get_arc(0).node_ptr.id == 101
    assert g.traders.get_node(4).get_arc(0).node_ptr.id == 102
    assert g.currencies.get_node(101).get_arc(0).node_ptr.id == 4
    assert g.currencies.get_node(101).get_arc(1).node_ptr.id == 1
    assert g.currencies.get_node(102).get_arc(0).node_ptr.id == 3
    assert g.currencies.get_node(102).get_arc(1).node_ptr.id == 2
    assert g.currencies.get_node(103).get_arc(0).node_ptr.id == 4


@pytest.mark.skip('performance test')
@cytest
def test_random_matches():
    import random
    import time
    from collections import namedtuple

    Currency = namedtuple("Currency", "id location")

    ################################
    # Graph generation parameters: #
    ################################

    # The processing time increases almost linearly with the increase
    # of `traders_count`. 1 million traders with 5 million offers are
    # processed for approximately 10 seconds on a 3GHz machine.
    traders_count = 1000000

    # When `traders_count` is fixed, the processing time increases
    # linearly with the increase of `offers_count`.
    offers_count = 5 * traders_count

    # Increasing the `currencies_count` increases the processing time,
    # but not by more than a few times.
    currencies_count = traders_count // 50

    # Decreasing the buyers/sellers ratio decreases the processing
    # time; and increasing the buyers/sellers ratio increases the
    # processing time.
    additional_sellers_to_buyers_ratio = 1 / 10

    # When `avg_sell_amount` and `avg_sell_amount` are increased
    # (compared to 1.0 which is the chosen currency `min_amount`), the
    # processing time also increases, but more and more slowly.
    avg_sell_amount = 100.0
    avg_buy_amount = avg_sell_amount

    # The processing time seems to not be significantly affected by
    # changes in `locality_distance`.
    locality_distance = min(traders_count, 10000)

    assert traders_count >= 2
    assert currencies_count >= 2
    random.seed(1)
    lambd_sell = 1 / avg_sell_amount
    lambd_buy = 1 / avg_buy_amount
    traders_list = [trader_id for trader_id in range(1, traders_count + 1)]
    currencies_list = [
        Currency(currency_id, random.randrange(traders_count))
        for currency_id in range(10000000001, currencies_count + 10000000001)
    ]

    graph = Digraph()
    for currency_id, _ in currencies_list:
        graph.add_currency(currency_id, 1.0)

    # Add one buy offer and one sell offer for each trader.
    for trader_id in traders_list:
        buy_currency = random.choice(currencies_list)
        buy_amount = random.expovariate(lambd_buy)
        sell_currency = random.choice(currencies_list)
        sell_amount = random.expovariate(lambd_sell)
        graph.add_supply(sell_amount, sell_currency.id, trader_id)
        graph.add_demand(buy_amount, buy_currency.id, trader_id)

    # Add additional buy/sell offers to reach `offers_count`.
    for _ in range(offers_count - 2 * traders_count):
        currency = random.choice(currencies_list)
        trader_location = (
            currency.location
            + random.randrange(locality_distance)
        )
        trader_id = traders_list[trader_location % traders_count]
        r = additional_sellers_to_buyers_ratio
        if random.random() < r / (r + 1):
            # Add a seller.
            amount = random.expovariate(lambd_sell)
            graph.add_supply(amount, currency.id, trader_id)
        else:
            # Add a buyer.
            amount = random.expovariate(lambd_buy)
            graph.add_demand(amount, currency.id, trader_id)

    zero_time = time.time()
    performed_deals = 0
    cleared_amount = 0
    for amount, nodes in graph.cycles():
        performed_deals += 1
        cleared_amount += (amount * len(nodes) // 2)

    print(performed_deals, cleared_amount, time.time() - zero_time)
