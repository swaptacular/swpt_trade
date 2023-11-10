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
    typedef unsigned int nodeflags;

    class Node;

    class Arc {
    public:
      Node* node_ptr;
      double amount;

      Arc() {
        node_ptr = NULL;
        amount = 0.0;
      }
      Arc(Node* node_ptr, double amount) {
        this->node_ptr = node_ptr;
        this->amount = amount;
      }
      Arc(const Arc& other) {
        node_ptr = other.node_ptr;
        amount = other.amount;
      }
    };

    class Node {
    private:
      std::vector<Arc> arcs;

    public:
      const i64 id;
      const double min_amount;
      nodeflags flags;

      Node(i64 id, double min_amount, nodeflags flags)
          : id(id), min_amount(min_amount) {
        this->flags = flags;
      }
      unsigned int arcs_count() {
        return arcs.size();
      }
      Arc& add_arc(Node* node_ptr, double amount) {
        arcs.push_back(Arc(node_ptr, amount));
        return arcs.back();
      }
      Arc& get_arc(size_t index) {
        return arcs.at(index);
      }
    };

    class NodeRegistry {
    private:
      std::unordered_map<i64, Node*> map;

    public:
      ~NodeRegistry() {
        for (auto pair = map.begin(); pair != map.end(); ++pair) {
          delete pair->second;
        }
      }
      Node* create_node(i64 id, double min_amount, nodeflags flags) {
        return map[id] = new Node(id, min_amount, flags);
      }
      Node* get_node(i64 id) {
        try {
            return map.at(id);
        } catch (const std::out_of_range& oor) {
            return NULL;
        }
      }
    };
    """
    ctypedef long long i64
    ctypedef unsigned int nodeflags

    cdef cppclass Arc:
        Node* node_ptr
        double amount
        Arc(Node*, double) except +
        Arc(const Arc&) except +

    cdef cppclass Node:
        const i64 id
        const double min_amount
        nodeflags flags
        Node(i64, double, nodeflags) except +
        unsigned int arcs_count() noexcept
        Arc& add_arc(Node*, double) except +
        Arc& get_arc(size_t) except +

    cdef cppclass NodeRegistry:
        NodeRegistry() except +
        Node* create_node(i64, double, nodeflags) except +
        Node* get_node(i64) noexcept


cdef class Digraph:
    cdef NodeRegistry debtors
    cdef NodeRegistry creditors

    def __cinit__(self):
        pass
        # self._vmap = {ROOT_VERTEX: []}

    cdef void add_supply(self, i64 debtor_id, double amount, i64 creditor_id):
        cdef Node* debtor_ptr = self.debtors.get_node(debtor_id)
        if debtor_ptr == NULL:
            raise RuntimeError("invalid debtor node")

        cdef Node* creditor_ptr = self.creditors.get_node(creditor_id)
        if creditor_ptr == NULL:
            creditor_ptr = creditors.create_node(creditor_id, 0.0, 0)

        creditor_ptr.add_arc(debtor_ptr, amount)

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


cdef NodeRegistry debtors
cdef NodeRegistry creditors


cpdef double dist((double, double) point1, (double, double) point2):
    cdef double x = (point1[0] - point2[0]) ** 2
    cdef double y = (point1[1] - point2[1]) ** 2
    return math.sqrt(x + y)


cdef double mysum(double x, double y):
    debtors.get_node(1)
    debtors.create_node(1, 0.0, 0)
    n_ptr = debtors.get_node(1)
    if n_ptr == NULL:
        print('not found')
    else:
        print(n_ptr.id)

    return x + y
