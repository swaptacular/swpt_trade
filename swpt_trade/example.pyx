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

    inline void* lookup_node(voidmap *map, i64 node_id) {
        try {
            return map->at(node_id);
        } catch (const std::out_of_range& oor) {
            return NULL;
        }
    }
    """
    cdef void* lookup_node(voidmap*, i64)

    cdef Node* add_node(nodemap* nodes_map, Node* node_ptr) except +:
        deref(nodes_map)[node_ptr.id] = node_ptr
        return node_ptr

    cdef Node* get_node(nodemap* nodes_map, i64 node_id) noexcept:
        return <Node*>lookup_node(<voidmap*>nodes_map, node_id)

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
    get_node(debtors, 1)
    cdef Node n = Node(1, vector[Arc](), 0.0, False)
    add_node(debtors, &n)
    n_ptr = get_node(debtors ,1)
    if n_ptr == NULL:
        print('not found')
    else:
        print(n_ptr.id)

    return x + y
