# distutils: language = c++

import pytest
import math
from . import cytest
from swpt_trade.matcher cimport Arc, Node, NodeRegistry, Digraph, INF_AMOUNT


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
def test_construct_digraph():
    g = Digraph()
    cdef Node* root = g.path.back()
    assert root != NULL
    assert root.arcs_count() == 0
    assert root.status == 1

    with pytest.raises(RuntimeError):
        g.add_supply(100.0, 666, 2)

    with pytest.raises(RuntimeError):
        g.add_demand(1, 100.0, 666)

    assert root.arcs_count() == 0
    g.add_currency(666, 100.0)
    assert root.arcs_count() == 1

    with pytest.raises(RuntimeError):
        g.add_currency(666, 100.0)

    g.add_supply(1000.0, 666, 1)
    g.add_supply(2000.0, 666, 2)
    g.add_demand(2, 500.0, 666)

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
