# distutils: language = c++

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
            return RuntimeError("The graph traversal has already started.")
        if self.currencies.get_node(currency_id) != NULL:
            raise RuntimeError("duplicated currency")
        if min_amount <= 0.0:
            raise RuntimeError("invalid min_amount")

        currency = self.currencies.create_node(
            currency_id, min_amount, NODE_INITIAL_STATUS
        )
        root_trader = self.path.front()
        root_trader.add_arc(currency, INF_AMOUNT)

    def add_supply(self, double amount, i64 currency_id, i64 seller_id):
        """Declares that a given seller wants to sell a given amount
        of a given currency.
        """
        if not self._is_pristine():
            return RuntimeError("The graph traversal has already started.")

        currency, seller = self._ensure_nodes(currency_id, seller_id)
        currency.add_arc(seller, amount)

    def add_demand(self, i64 buyer_id, double amount, i64 currency_id):
        """Declares that a given buyer wants to buy a given amount of
        a given currency.
        """
        if not self._is_pristine():
            return RuntimeError("The graph traversal has already started.")

        currency, buyer = self._ensure_nodes(currency_id, buyer_id)
        buyer.add_arc(currency, amount)

    def find_cycle(self):
        # TODO
        pass

    cdef bool _find_cylce(self) except? False:
        cdef Node* current_node
        cdef Node* next_node
        cdef size_t arcs_count
        cdef size_t next_arc_index
        cdef Arc* next_arc

        while self.path.size() > 0:
            current_node = self.path.back()
            arcs_count = current_node.arcs_count()
            next_arc_index = (
                (current_node.status >> 1)
                # If the "path" flag of the current node is set, this
                # means that the arc that we followed turned out to be
                # a dead end. Therefore, now we must skip it.
                + (current_node.status & NODE_PATH_FLAG)
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
                next_arc_index += 1  # The arc is invalid. Skip it.

            if next_node == NULL:
                # The current node is a dead end.
                current_node.status = next_arc_index << 1
                self.path.pop_back()
            elif next_node.status & NODE_PATH_FLAG == 0:
                # We follow the arc, moving to the next node.
                current_node.status = (next_arc_index << 1) | NODE_PATH_FLAG
                self.path.push_back(next_node)
            else:
                # We've got a cycle!
                current_node.status = next_arc_index << 1
                return True

        # There are no cycles.
        return False

    cdef bool _is_pristine(self) noexcept:
        return (
            self.path.size() == 1
            and self.path[0].status == NODE_INITIAL_STATUS
        )

    cdef (Node*, Node*) _ensure_nodes(self, i64 currency_id, i64 trader_id):
        currency = self.currencies.get_node(currency_id)
        if currency == NULL:
            raise RuntimeError("invalid currency")

        trader = self.traders.get_node(trader_id)
        if trader == NULL:
            trader = self.traders.create_node(
                trader_id, 0.0, NODE_INITIAL_STATUS
            )
        return currency, trader
