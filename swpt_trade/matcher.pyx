# distutils: language = c++

cdef nodeflags CURRENCY_FLAGS_DEFAULT = 0
cdef nodeflags TRADER_FLAGS_DEFAULT = 0
cdef i64 ROOT_TRADER_ID = 0


cdef class Digraph:
    def __cinit__(self):
        self.root_trader = self.traders.create_node(
            ROOT_TRADER_ID, 0.0, TRADER_FLAGS_DEFAULT
        )

    def add_currency(self, i64 currency_id, double min_amount):
        if self.currencies.get_node(currency_id) != NULL:
            raise RuntimeError("duplicated currency")
        if min_amount <= 0.0:
            raise RuntimeError("invalid min_amount")

        currency = self.currencies.create_node(
            currency_id, min_amount, CURRENCY_FLAGS_DEFAULT
        )
        self.root_trader.add_arc(currency, INF_AMOUNT)

    def add_supply(self, double amount, i64 currency_id, i64 seller_id):
        currency, seller = self._ensure_nodes(currency_id, seller_id)
        currency.add_arc(seller, amount)

    def add_demand(self, i64 buyer_id, double amount, i64 currency_id):
        currency, buyer = self._ensure_nodes(currency_id, buyer_id)
        buyer.add_arc(currency, amount)

    cdef (Node*, Node*) _ensure_nodes(self, i64 currency_id, i64 trader_id):
        currency = self.currencies.get_node(currency_id)
        if currency == NULL:
            raise RuntimeError("invalid currency")

        trader = self.traders.get_node(trader_id)
        if trader == NULL:
            trader = self.traders.create_node(
                trader_id, 0.0, TRADER_FLAGS_DEFAULT
            )
        return currency, trader
