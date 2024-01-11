from cpython cimport array
from libc.math cimport NAN
from libcpp cimport bool
from libcpp.unordered_set cimport unordered_set
from swpt_trade.pricing cimport distance, i64
from swpt_trade.pricing import (
    BidProcessor,
    DEFAULT_MAX_DISTANCE_TO_BASE,
    DEFAULT_MIN_TRADE_AMOUNT,
)
from swpt_trade.matching import Digraph


cdef class Solver:
    """Builds a digraph, then finds the cycles and aggregates them.
    """
    cdef readonly str base_debtor_info_uri
    cdef readonly i64 base_debtor_id
    cdef readonly distance max_distance_to_base
    cdef readonly i64 min_trade_amount
    cdef object bid_processor
    cdef object graph
    cdef unordered_set[i64] debtor_ids

    def __cinit__(
        self,
        str base_debtor_info_uri,
        i64 base_debtor_id,
        distance max_distance_to_base=DEFAULT_MAX_DISTANCE_TO_BASE,
        i64 min_trade_amount=DEFAULT_MIN_TRADE_AMOUNT,
    ):
        assert base_debtor_id != 0
        assert max_distance_to_base > 0
        assert min_trade_amount > 0
        self.base_debtor_info_uri = base_debtor_info_uri
        self.base_debtor_id = base_debtor_id
        self.max_distance_to_base = max_distance_to_base
        self.min_trade_amount = min_trade_amount
        self.bid_processor = BidProcessor(
            base_debtor_info_uri,
            base_debtor_id,
            max_distance_to_base,
            min_trade_amount,
        )
        self.graph = Digraph()

    def register_debtor(
        self,
        bool confirmed,
        str debtor_info_uri,
        i64 debtor_id,
        str peg_debtor_info_uri='',
        i64 peg_debtor_id=0,
        float peg_exchange_rate=NAN,
    ):
        self.bid_processor.register_currency(
            confirmed,
            debtor_info_uri,
            debtor_id,
            peg_debtor_info_uri,
            peg_debtor_id,
            peg_exchange_rate,
        )
        self.debtor_ids.insert(debtor_id)

    def analyze_debtors(self):
        cdef double min_trade_amount = float(self.min_trade_amount)
        cdef double price
        self.bid_processor.analyze_bids()
        for debtor_id in self.debtor_ids:
            price = self.bid_processor.get_currency_price(debtor_id)
            if price > 0.0:
                self.graph.add_currency(debtor_id,  min_trade_amount * price)
        self.debtor_ids.clear()

    def add_supply(
        self,
        i64 creditor_id,
        i64 debtor_id,
        i64 amount,
        i64 collector_id,
    ):
        cdef double price = self.bid_processor.get_currency_price(debtor_id)
        if price > 0.0:
            self.graph.add_supply(amount * price, debtor_id, creditor_id)
            # TODO: save collector_id

    def add_demand(self, i64 creditor_id, i64 debtor_id, i64 amount):
        cdef double price = self.bid_processor.get_currency_price(debtor_id)
        if price > 0.0:
            self.graph.add_demand(amount * price, debtor_id, creditor_id)
