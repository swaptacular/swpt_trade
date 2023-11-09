# distutils: language = c++

from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from cython.operator cimport dereference as deref
import math

ctypedef long long i64
ctypedef unordered_map[i64, Node*] nodemap
ctypedef unordered_map[i64, void*] voidmap


cdef struct Node:
    i64 id
    vector[Arc] arcs
    double min_amount
    bint visited


cdef struct Arc:
    Node* node_ptr
    double amount


cdef nodemap* debtors = new nodemap()
cdef nodemap* creditors = new nodemap()


cdef extern from *:
    """
    #include <unordered_map>
    #include <stdexcept>

    typedef long long i64;
    typedef std::unordered_map<long long, void*> voidmap;

    void* lookup_node(voidmap *map, i64 node_id) {
        try {
            return map->at(node_id);
        } catch (const std::out_of_range& oor) {
            return NULL;
        }
    }
    """
    cdef void* lookup_node(voidmap*, i64)

    cdef Node* get_debtor(nodemap* debtors_map, i64 debtor_id) noexcept:
        return <Node*>lookup_node(<voidmap*>debtors_map, debtor_id)

    cdef Node* add_debtor(nodemap* debtors_map, Node* debtor) except +:
        deref(debtors_map)[debtor.id] = debtor
        return debtor

    cdef Node* get_creditor(nodemap* creditors_map, i64 creditor_id) noexcept:
        return <Node*>lookup_node(<voidmap*>creditors_map, creditor_id)

    cdef Node* add_creditor(nodemap* creditors_map, Node* creditor) except +:
        deref(creditors)[creditor.id] = creditor
        return creditor

    cdef Arc* add_arc(Node* node, Arc arc) except +:
        node.arcs.push_back(arc)
        return &arc

    cdef Arc* get_arc(Node* node, size_t index) except +:
        return &node.arcs.at(index)


cpdef double dist((double, double) point1, (double, double) point2):
    cdef double x = (point1[0] - point2[0]) ** 2
    cdef double y = (point1[1] - point2[1]) ** 2
    return math.sqrt(x + y)

cdef double mysum(double x, double y):
    get_debtor(debtors, 1)
    cdef Node n = Node(1, vector[Arc](), 0.0, False)
    add_debtor(debtors, &n)
    n_ptr = get_debtor(debtors ,1)
    if n_ptr == NULL:
        print('not found')
    else:
        print(n_ptr.id)

    return x + y
