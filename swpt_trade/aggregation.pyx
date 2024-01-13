# distutils: language = c++
import cython
from cython.operator cimport dereference as deref, postincrement
from libc.stdlib cimport rand
from libc.math cimport NAN
from libcpp cimport bool
from swpt_trade.pricing cimport distance, BidProcessor
from swpt_trade.pricing import (
    DEFAULT_MAX_DISTANCE_TO_BASE,
    DEFAULT_MIN_TRADE_AMOUNT,
)
from swpt_trade.matching cimport Digraph
from collections import namedtuple


AccountChange = namedtuple(
    'AccountChange', ['creditor_id', 'debtor_id', 'amount', 'collector_id']
)


cdef class Solver:
    """Builds a digraph, then finds the cycles and aggregates them.
    """
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

    def register_currency(
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

    def analyze_currencies(self):
        cdef double min_trade_amount = float(self.min_trade_amount)
        cdef double price
        self.bid_processor.analyze_bids()
        for debtor_id in self.debtor_ids:
            price = self.bid_processor.get_currency_price(debtor_id)
            if price > 0.0:
                self.graph.add_currency(debtor_id,  min_trade_amount * price)
        self.debtor_ids.clear()

    def register_collector_account(self, i64 creditor_id, i64 debtor_id):
        self.collector_accounts.insert(
            CollectorAccount(creditor_id, debtor_id)
        )

    def register_sell_offer(
        self,
        i64 creditor_id,
        i64 debtor_id,
        i64 amount,
        i64 collector_id,
    ):
        cdef double price = self.bid_processor.get_currency_price(debtor_id)
        if price > 0.0:
            self.graph.add_supply(amount * price, debtor_id, creditor_id)
            seller_account = Account(creditor_id, debtor_id)
            self.changes[seller_account] = AccountData(0, collector_id)

    def register_buy_offer(
        self,
        i64 creditor_id,
        i64 debtor_id,
        i64 amount,
    ):
        cdef double price = self.bid_processor.get_currency_price(debtor_id)
        if price > 0.0:
            self.graph.add_demand(amount * price, debtor_id, creditor_id)

    def analyze_offers(self):
        cdef double amount
        cdef i64[:] cycle

        for amount, cycle in self.graph.cycles():
            self._process_cycle(amount, cycle)

    def takings_iter(self):
        t = AccountChange
        for pair in self.changes:
            account = pair.first
            data = pair.second
            if data.amount_change < 0:
                yield t(
                    account.creditor_id,
                    account.debtor_id,
                    data.amount_change,
                    data.collector_id,
                )

    def givings_iter(self):
        t = AccountChange
        for pair in self.changes:
            account = pair.first
            data = pair.second
            if data.amount_change > 0:
                yield t(
                    account.creditor_id,
                    account.debtor_id,
                    data.amount_change,
                    data.collector_id,
                )

    cdef void _process_cycle(self, double amount, i64[:] cycle):
        cdef i64 n = len(cycle)
        cdef i64 i = 0
        cdef i64 debtor_id
        cdef double price
        cdef i64 amt
        cdef AccountData* giver_data
        cdef AccountData* taker_data

        while i < n:
            debtor_id = cycle[i]
            price = self.bid_processor.get_currency_price(debtor_id)
            amt = int(amount / price)  # TODO: check!

            giver_data = &self.changes[Account(cycle[i - 1], debtor_id)]
            giver_data.amount_change -= amt
            if giver_data.collector_id == 0:
                raise RuntimeError("invalid collector_id")

            taker_data = &self.changes[Account(cycle[i + 1], debtor_id)]
            taker_data.amount_change += amt
            if taker_data.collector_id == 0:
                taker_data.collector_id = self._get_random_collector_id(
                    giver_data.collector_id, debtor_id
                )

            self._update_collector(giver_data.collector_id, debtor_id, +amt)
            self._update_collector(taker_data.collector_id, debtor_id, -amt)
            i += 2

    cdef void _update_collector(self, i64 creditor_id, i64 debtor_id, i64 amt):
        cdef Account account = Account(creditor_id, debtor_id)
        cdef i64* amount_ptr = &self.collection_amounts[account]
        amount_ptr[0] = deref(amount_ptr) + amt

    @cython.cdivision(True)
    cdef i64 _get_random_collector_id(
        self,
        i64 giver_collector_id,
        i64 debtor_id
    ):
        account = CollectorAccount(giver_collector_id, debtor_id)
        cdef size_t count = self.collector_accounts.count(account)
        cdef int n

        if count == 1:
            # There is only one matching collector account. (This
            # should be by far the most common case.)
            return deref(self.collector_accounts.find(account)).creditor_id
        elif count > 1:
            # Randomly select one of the matching collector accounts.
            n = rand() % count
            pair = self.collector_accounts.equal_range(account)
            it = pair.first
            while n > 0:
                postincrement(it)
                n -= 1
            return deref(it).creditor_id
        else:
            # There are no matching collector accounts. Normally this
            # should never happen. Nevertheless, using the giver's
            # collector account to pay the taker seems to be a pretty
            # reliable fallback.
            return giver_collector_id
