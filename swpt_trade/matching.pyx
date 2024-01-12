# distutils: language = c++
from libcpp.vector cimport vector
from cpython cimport array

# To ensure that we will traverse all the nodes in the graph, we
# create an artificial node called "the root trader", which wants to
# buy infinite amounts of all existing currencies. The traversal of
# the graph always starts from the root trader.
cdef i64 ROOT_TRADER_ID = 0

# The node's status is a "size_t" word (64 bits on 64-bit machines, 32
# bits on 32-bit machines), which contains two pieces of information:
#
# 1) The index of the currently traversed arc.
# 2) A flag indicating that the node participates in the currently
#    traversed path.
#
# The word's least significant bit holds the flag, and the highest 63
# bits hold the index:
#
#   63    62    61   .  .  .  .  .  .  .  .  3     2     1     0
# +---------------------------------------------------------+-----+
# |                                                         |     |
# |                    index (63 bits)                      |1 bit|
# |                                                         |flag |
# +---------------------------------------------------------+-----+
#
# Initially, all status bits are zeroed (index = 0, flag = 0).
cdef nodestatus NODE_INITIAL_STATUS = 0
cdef nodestatus NODE_PATH_FLAG = 1


cdef class Digraph:
    """A directed graph of trading offers.
    """

    def __cinit__(self):
        root_trader = self.traders.create_node(
            ROOT_TRADER_ID, 0.0, NODE_INITIAL_STATUS
        )
        self.path.push_back(root_trader)

    def add_currency(self, i64 currency_id, double min_amount):
        """Declares a currency.

        All arranged trades in this currency will be for amounts
        greater of equal than the specified `min_amount`. Possible
        trades for lesser amounts will be ignored.
        """
        if not self._is_pristine():
            raise RuntimeError("The graph traversal has already started.")
        if self.currencies.get_node(currency_id) != NULL:
            raise ValueError("duplicated currency")
        if not min_amount > 0.0:
            raise ValueError("invalid min_amount")

        currency = self.currencies.create_node(
            currency_id, min_amount, NODE_INITIAL_STATUS
        )
        root_trader = self.path.front()
        root_trader.add_arc(currency, INF_AMOUNT)

    def get_min_amount(self, i64 currency_id):
        """Return the `min_amount` set for a given currency.

        Raises a `ValueError` if the passed currency ID does not
        correspond to a known currency.
        """
        currency = self.currencies.get_node(currency_id)
        if currency == NULL:
            raise ValueError("invalid currency")

        return currency.min_amount

    cpdef void add_supply(self, double amount, i64 currency_id, i64 seller_id):
        """Declares that a given seller wants to sell a given amount
        of a given currency.
        """
        if not self._is_pristine():
            raise RuntimeError("The graph traversal has already started.")
        if seller_id == ROOT_TRADER_ID:
            raise ValueError("invalid seller ID")

        currency, seller = self._ensure_nodes(currency_id, seller_id)
        currency.add_arc(seller, amount)

    cpdef void add_demand(self, double amount, i64 currency_id, i64 buyer_id):
        """Declares that a given buyer wants to buy a given amount of
        a given currency.
        """
        if not self._is_pristine():
            raise RuntimeError("The graph traversal has already started.")

        currency, buyer = self._ensure_nodes(currency_id, buyer_id)
        buyer.add_arc(currency, amount)

    def find_cycle(self):
        """Try to find a trading cycle in the graph.

        Returns an (amount, node_ids_sequence) tuple when a cycle has
        been found; otherwise returns `None`. The amount will always
        be a positive float (it could be `math.inf` though). The
        returned sequence of node IDs will consist of alternating
        currency IDs and trader IDs, and the first element in the
        sequence will always be a currency ID. The total number of
        elements in the sequence will always be even. For example:

        >>> graph.find_cycle()
        (10.0, array('q', [101, 1, 102, 2]))

        means that:

        1. The trader with ID `1` should receive 10.0 tokens of the
           `101` currency, and should give 10.0 tokens of the `102`
           currency.

        2. The trader with ID `2` should receive 10.0 tokens of the
           `102` currency, and should give 10.0 tokens of the `101`
           currency.

        Note that this method modifies the graph, so that next calls
        to `graph.find_cycle()` will yield another trading cycle, or
        no cycle. For example, the result of calling `find_cycle()`
        again may be:

        >>> graph.find_cycle()
        None

        Once a `None` has been returned, next calls to `find_cycle()`
        will also return `None`. This means that all possible trading
        cycles have been exhausted.
        """
        if self._is_pristine():
            # NOTE: Before we start, we must optimize the order in
            # which each node's arcs are traversed. The goal is to
            # visit nodes with bigger branching factor first, thus
            # minimizing the average length of the generated cycles.
            self._sort_arcs()

        if self._find_cycle():
            return self._process_cycle()

        return None

    def cycles(self):
        """Iterate over all trading cycles in the graph.

        Note that once the returned iterator has been exhausted, next
        calls to `graph.cycles()` will return an empty iterator.
        """
        while cycle := self.find_cycle():
            yield cycle

    cdef object _process_cycle(self):
        cdef Node* current_node = self.path.back()
        cdef size_t offset = 1 if current_node.min_amount > 0.0 else 0
        cdef size_t arc_index = current_node.status >> 1
        cdef Node* last_node = current_node.get_arc(arc_index).node_ptr
        cdef vector[Arc*] arcs
        arcs.reserve(1000)

        cdef Arc* arc
        cdef double cycle_amount = INF_AMOUNT

        while True:
            arc = &current_node.get_arc(arc_index)
            arcs.push_back(arc)

            if arc.amount < cycle_amount:
                cycle_amount = arc.amount

            if current_node == last_node:
                break

            self.path.pop_back()
            current_node = self.path.back()
            arc_index = current_node.status >> 1
            current_node.status = arc_index << 1  # Clears the "path" flag.

        cdef size_t cycle_length  = arcs.size()
        cdef array.array cycle_array = array.array('q')
        array.resize(cycle_array, cycle_length)
        cdef i64[:] ca = cycle_array
        cdef size_t i

        for i in range(cycle_length):
            arc = arcs[i]
            arc.amount -= cycle_amount
            ca[(i + offset) % cycle_length] = arc.node_ptr.id

        return cycle_amount, cycle_array

    cdef bool _find_cycle(self) except? False:
        cdef Node* current_node
        cdef Node* next_node
        cdef size_t arcs_count
        cdef size_t next_arc_index
        cdef Arc* next_arc
        cdef nodestatus path_flag = NODE_PATH_FLAG

        while self.path.size() > 0:
            current_node = self.path.back()
            arcs_count = current_node.arcs_count()
            next_arc_index = (
                (current_node.status >> 1)
                # If the "path" flag of the current node is set, this
                # means that the arc that we followed turned out to be
                # a dead end. Therefore, now we must skip it.
                + (current_node.status & path_flag)
            )
            next_node = NULL

            while next_arc_index < arcs_count:
                next_arc = &current_node.get_arc(next_arc_index)
                amount = next_arc.amount
                if (
                    amount >= current_node.min_amount
                    and amount >= next_arc.node_ptr.min_amount
                ):
                    next_node = next_arc.node_ptr
                    break
                next_arc_index += 1  # Invalid arc. Skip it.

            if next_node == NULL:
                # The current node is a dead end.
                current_node.status = next_arc_index << 1
                self.path.pop_back()
            elif next_node.status & path_flag == 0:
                # We follow the arc, moving to the next node.
                current_node.status = (next_arc_index << 1) | path_flag
                self.path.push_back(next_node)
            else:
                # We've got a cycle!
                current_node.status = next_arc_index << 1
                return True

        # There are no cycles.
        return False

    cdef inline bool _is_pristine(self) noexcept:
        return (
            self.path.size() == 1
            and self.path[0].status == NODE_INITIAL_STATUS
        )

    cdef (Node*, Node*) _ensure_nodes(self, i64 currency_id, i64 trader_id):
        currency = self.currencies.get_node(currency_id)
        if currency == NULL:
            raise ValueError("invalid currency")

        trader = self.traders.get_node(trader_id)
        if trader == NULL:
            trader = self.traders.create_node(
                trader_id, 0.0, NODE_INITIAL_STATUS
            )
        return currency, trader

    cdef void _sort_arcs(self):
        self.currencies.calc_ranks()
        self.traders.calc_ranks()

        self.currencies.sort_arcs()
        self.traders.sort_arcs()
