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
        root_trader = self.path.back()
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
        if self._is_pristine():
            self.path[0].status = 1

        # TODO

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
