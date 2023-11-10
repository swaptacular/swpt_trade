# distutils: language = c++

import pytest
from . import cytest
from swpt_trade.matcher cimport Arc, Node, NodeRegistry


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
    assert node.flags == 3
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
    cdef NodeRegistry debtors
    assert debtors.get_node(1) == NULL
    assert debtors.create_node(1, 100.0, 3) != NULL
    node_ptr = debtors.get_node(1)
    assert node_ptr != NULL
    assert node_ptr.id == 1
    assert node_ptr.min_amount == 100.0
    assert node_ptr.flags == 3
