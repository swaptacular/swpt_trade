# distutils: language = c++

cdef extern from *:
    """
    #ifndef MATCHER_CLASSES_H
    #define MATCHER_CLASSES_H

    #include <unordered_map>
    #include <vector>
    #include <stdexcept>
    #include <limits>

    typedef long long i64;
    typedef unsigned int nodeflags;
    const double INF_AMOUNT = std::numeric_limits<double>::infinity();

    class Node;

    class Arc {
    public:
      Node* node_ptr;
      double amount;

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
    #endif
    """
    ctypedef long long i64
    ctypedef unsigned int nodeflags
    cdef double INF_AMOUNT

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
    cdef NodeRegistry currencies
    cdef NodeRegistry traders
    cdef Node* root_trader
    cdef (Node*, Node*) _ensure_nodes(self, i64, i64)

