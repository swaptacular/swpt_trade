# distutils: language = c++

from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from cython.operator cimport dereference as deref
import math

cdef extern from *:
    """
    #include <unordered_map>
    #include <vector>
    #include <stdexcept>

    typedef long long i64;

    class Node;

    class Arc {
    public:
      Node* node_ptr;
      double amount;

      Arc() {}
      Arc(Node* node_ptr, double amount) {
        node_ptr = node_ptr;
        amount = amount;
      }
      Arc(const Arc& other) {
        node_ptr = other.node_ptr;
        amount = other.amount;
      }
      ~Arc() {}
    };

    class Node {
    public:
      i64 id;
      std::vector<Arc> arcs;
      double min_amount;
      int flags;

      Node() {}
      Node(i64 id, std::vector<Arc> arcs, double min_amount, int flags) {
        id = id;
        arcs = arcs;
        min_amount = min_amount;
        flags = flags;
      }
      ~Node() {}
    };

    typedef std::unordered_map<i64, Node*> nodemap;

    inline Node* lookup_node(nodemap *map, i64 node_id) {
        try {
            return map->at(node_id);
        } catch (const std::out_of_range& oor) {
            return NULL;
        }
    }
    """
    ctypedef long long i64

    cdef cppclass Arc:
        Node* node_ptr
        double amount
        Arc() except +
        Arc(Node*, double) except +
        Arc(const Arc&) except +

    cdef cppclass Node:
        i64 id
        vector[Arc] arcs
        double min_amount
        int flags
        Node() except +
        Node(i64, vector[Arc], double, int) except +

    ctypedef unordered_map[i64, Node*] nodemap

    cdef Node* lookup_node(nodemap*, i64)


cdef Node* add_node(nodemap* nodes_map, Node* node_ptr) except +:
    deref(nodes_map)[node_ptr.id] = node_ptr
    return node_ptr

cdef Node* get_node(nodemap* nodes_map, i64 node_id) noexcept:
    return lookup_node(nodes_map, node_id)

cdef Arc* add_arc(Node* node, Arc arc) except +:
    node.arcs.push_back(arc)
    return &arc

cdef Arc* get_arc(Node* node, size_t index) except +:
    return &node.arcs.at(index)


cdef nodemap* debtors = new nodemap()
cdef nodemap* creditors = new nodemap()


cdef class Digraph:
    cdef nodemap* debtors
    cdef nodemap* creditors

    def __cinit__(self):
        self.debtors = new nodemap()
        self.creditors = new nodemap()
        # self._vmap = {ROOT_VERTEX: []}

    cdef void add_supply(self, i64 debtor_id, double amount, i64 creditor_id):
        cdef Node* debtor_ptr = get_node(self.debtors, debtor_id)
        if debtor_ptr == NULL:
            raise RuntimeError("invalid debtor node")

        cdef Node* node_ptr
        cdef Node* creditor_ptr = get_node(self.creditors, creditor_id)
        if creditor_ptr == NULL:
            node_ptr = new Node(creditor_id, vector[Arc](), 0.0, 0)
            creditor_ptr = add_node(self.creditors, node_ptr)

        add_arc(debtor_ptr, Arc(creditor_ptr, amount))

        # assert v is not None
        # assert v != ROOT_VERTEX
        # if u in self._vmap:
        #     self._vmap[u].append(v)
        # else:
        #     self._vmap[u] = [v]
        #     self._vmap[ROOT_VERTEX].append(u)

    cdef void add_demand(self, i64 creditor_id, i64 debtor_id, double amount):
        pass
        # assert v is not None
        # assert v != ROOT_VERTEX
        # if u in self._vmap:
        #     self._vmap[u].append(v)
        # else:
        #     self._vmap[u] = [v]
        #     self._vmap[ROOT_VERTEX].append(u)

    def remove_arc(self, u, v):
        pass
        # assert u != ROOT_VERTEX
        # try:
        #     vlist = self._vmap[u]
        #     vlist[vlist.index(v)] = None
        # except (KeyError, ValueError):
        #     pass

    def _sink_vertex(self, v):
        pass
        # if v != ROOT_VERTEX:
        #     try:
        #         del self._vmap[v]
        #     except KeyError:
        #         pass


cpdef double dist((double, double) point1, (double, double) point2):
    cdef double x = (point1[0] - point2[0]) ** 2
    cdef double y = (point1[1] - point2[1]) ** 2
    return math.sqrt(x + y)

cdef double mysum(double x, double y):
    get_node(debtors, 1)
    add_node(debtors, new Node(1, vector[Arc](), 0.0, False))
    n_ptr = get_node(debtors ,1)
    if n_ptr == NULL:
        print('not found')
    else:
        print(n_ptr.id)

    return x + y
