# distutils: language = c++

cdef nodestatus NODE_INITIAL_STATUS = 0
cdef i64 ROOT_TRADER_ID = 0


cdef class Digraph:
    def __cinit__(self):
        root_trader = self.traders.create_node(
            ROOT_TRADER_ID, 0.0, NODE_INITIAL_STATUS
        )
        self.path.push_back(root_trader)

    def add_currency(self, i64 currency_id, double min_amount):
        if not self._is_pristine():
            return RuntimeError("invalid state")
        if self.currencies.get_node(currency_id) != NULL:
            raise RuntimeError("duplicated currency")
        if min_amount <= 0.0:
            raise RuntimeError("invalid min_amount")

        currency = self.currencies.create_node(
            currency_id, min_amount, NODE_INITIAL_STATUS
        )
        self.path.back().add_arc(currency, INF_AMOUNT)

    def add_supply(self, double amount, i64 currency_id, i64 seller_id):
        if not self._is_pristine():
            return RuntimeError("invalid state")

        currency, seller = self._ensure_nodes(currency_id, seller_id)
        currency.add_arc(seller, amount)

    def add_demand(self, i64 buyer_id, double amount, i64 currency_id):
        if not self._is_pristine():
            return RuntimeError("invalid state")

        currency, buyer = self._ensure_nodes(currency_id, buyer_id)
        buyer.add_arc(currency, amount)

    def find_cycle(self):
        # TODO
        pass

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
