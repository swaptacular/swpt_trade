# distutils: language = c++
import cython
from cython.operator cimport dereference as deref, postincrement
from libcpp.utility cimport pair as Pair
from libcpp.unordered_set cimport unordered_set
from libcpp.unordered_map cimport unordered_multimap
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


AccountChange = namedtuple('AccountChange', [
    'creditor_id',
    'debtor_id',
    'amount',
    'collector_id',
])


CollectorTransfer = namedtuple('CollectorTransfer', [
    'debtor_id',
    'from_creditor_id',
    'to_creditor_id',
    'amount',
])


cdef class Solver:
    """Builds a digraph, then finds the cycles and aggregates them.

    The currencies are organized in a tree-like structure, each
    currency pointing to its peg currency. At the root of the tree is
    the "base currency". The base currency is identified by a ("debtor
    info URI", "debtor ID") pair.

    Currencies that are separated from the base currency by more than
    `max_distance_to_base` pegs will be ignored, and will not be
    included in the currency tree.

    All suggested trades will be for amounts greater or equal than the
    specified `min_trade_amount`. Possible trades for lesser amounts
    will be ignored.

    Usage example:

    >>> s = Solver('https://example.com/101', 101)

    Registering currencies:

    >>> s.register_currency(True, 'https://example.com/101', 101)
    >>> s.register_currency(True, 'https://example.com/102', 102,
    ...   'https://example.com/101', 101, 2.0)

    Registering collector accounts:

    >>> s.register_collector_account(998, 101)
    >>> s.register_collector_account(998, 102)

    Sell and buy offer can be registered in any order, but this must
    happen only after all the currencies, and all the collector
    accounts have been registered.

    >>> s.register_sell_offer(1, 101, 8000, 999)
    >>> s.register_buy_offer(1, 102, 6000)
    >>> s.register_sell_offer(2, 102, 3000, 999)
    >>> s.register_buy_offer(2, 101, 5000)

    Only after all sell and buy offers have been registered, we can
    see all the amounts that should be taken from sellers. These
    amounts will be received to the corresponding collector accounts:

    >>> list(s.takings_iter())
    [AccountChange(
       creditor_id=2, debtor_id=102, amount=-2500, collector_id=999),
     AccountChange(
       creditor_id=1, debtor_id=101, amount=-5000, collector_id=999)]

    Note that when there are lots and lots of registered offers, the
    first call to `s.takings_iter()`, `s.collector_transfers_iter()`,
    or `s.givings_iter()` may take a significant amount of time,
    during which the offers will be analyzed.

    To see all the transfers that must be performed between collector
    accounts before we start giving to buyers:

    >>> list(s.collector_transfers_iter())
    [CollectorTransfer(
       debtor_id=102, from_creditor_id=999, to_creditor_id=998, amount=2500),
     CollectorTransfer(
        debtor_id=101, from_creditor_id=999, to_creditor_id=998, amount=5000)]

    An finally, we can see all the amounts that should be given to
    buyers. These amounts will be taken the corresponding collector
    accounts:

    >>> list(s.givings_iter())
    [AccountChange(
       creditor_id=2, debtor_id=101, amount=5000, collector_id=998),
     AccountChange(
       creditor_id=1, debtor_id=102, amount=2500, collector_id=998)]
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
        self.currencies_analysis_done = False
        self.offers_analysis_done = False

    def register_currency(
        self,
        bool confirmed,
        str debtor_info_uri,
        i64 debtor_id,
        str peg_debtor_info_uri='',
        i64 peg_debtor_id=0,
        float peg_exchange_rate=NAN,
    ):
        """Register a currency, which might be pegged to another
        currency.

        When the `confirmed` flag is `True`, this means that a system
        account has been successfully created in this currency, and
        the currency's debtor info document has been confirmed as
        correct.

        Both the pegged currency and the peg currency are identified
        by a ("debtor info URI", "debtor ID") pair.

        The given `peg_exchange_rate` specifies the exchange rate
        between the pegged currency and the peg currency. For example,
        `2.0` would mean that pegged currency's tokens are twice as
        valuable as peg currency's tokens. Note that 0.0, +inf, -inf,
        and NAN are also acceptable exchange rate values.
        """
        if self.currencies_analysis_done:
            raise RuntimeError(
                "A currency has been registered after currencies analysis."
            )

        self.bid_processor.register_currency(
            confirmed,
            debtor_info_uri,
            debtor_id,
            peg_debtor_info_uri,
            peg_debtor_id,
            peg_exchange_rate,
        )
        self.debtor_ids.insert(debtor_id)

    def register_collector_account(self, i64 creditor_id, i64 debtor_id):
        """Registers a collector account.

        Collector accounts are used to receive traded amounts from
        sellers, so that they can be later transferred to the
        buyer(s).

        Normally, at least one collector account should be registered
        for each tradable currency (each currency is uniquely
        identified a `debtor_id`). However, if no collector accounts
        are registered for some or all of the traded currencies, the
        sell offers' `collector_id` parameters (see the
        `register_sell_offer` method) will determine which collector
        account will be used for each trade.

        When more than one collector accounts are registered for a
        given currency, the outgoing transfers to the buyers of this
        currency will be evenly distributed between all collector
        accounts.
        """
        if self.currencies_analysis_done:
            raise RuntimeError(
                "A collector account has been registered after currencies"
                " analysis."
            )

        self.collector_accounts.insert(
            CollectorAccount(creditor_id, debtor_id)
        )

    def register_sell_offer(
        self,
        i64 seller_creditor_id,
        i64 debtor_id,
        i64 amount,
        i64 collector_id,
    ):
        """Declares that a given seller (`seller_creditor_id`) wants
        to sell a given amount of a given currency (`debtor_id`).

        The `amount` should be positive.

        The `collector_id` argument is the creditor ID of the
        collector account that will eventually receive the traded
        amount from the seller, so that it can be transferred to the
        buyer(s).
        """
        if self.offers_analysis_done:
            raise RuntimeError(
                "A sell offer has been registered after offer analysis."
            )
        if not self.currencies_analysis_done:
            self._analyze_currencies()

        cdef double price = self.bid_processor.get_currency_price(debtor_id)
        if price > 0.0:
            self.graph.add_supply(
                amount * price, debtor_id, seller_creditor_id
            )
            seller_account = Account(seller_creditor_id, debtor_id)
            self.changes[seller_account] = AccountData(0, collector_id)

    def register_buy_offer(
        self,
        i64 buyer_creditor_id,
        i64 debtor_id,
        i64 amount,
    ):
        """Declares that a given buyer (`buyer_creditor_id`) wants to
        buy a given amount of a given currency (`debtor_id`).

        The `amount` should be positive.
        """
        if self.offers_analysis_done:
            raise RuntimeError(
                "A buy offer has been registered after offer analysis."
            )
        if not self.currencies_analysis_done:
            self._analyze_currencies()

        cdef double price = self.bid_processor.get_currency_price(debtor_id)
        if price > 0.0:
            self.graph.add_demand(amount * price, debtor_id, buyer_creditor_id)

    def takings_iter(self):
        """Iterate over the amounts that should be taken from the
        sellers.

        Each returned item will be a `AccountChange` namedtuple
        containing four `i64` numbers. The `amount` field specifies
        the amount that will be taken, and it will always be a
        negative number.
        """
        if not self.offers_analysis_done:
            self._analyze_offers()

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

    def collector_transfers_iter(self):
        """Iterate over the transfers that must be performed between
        collector accounts, so that at the end, each collector account
        receives exactly the same amount as it should give to buyers.

        Each returned item will be `CollectorTransfer` namedtuple
        containing four `i64` numbers. The `amount` field specifies
        the amount that should be transferred, and it will always be a
        positive number.
        """
        if not self.offers_analysis_done:
            self._analyze_offers()

        t = CollectorTransfer
        it = self.collector_transfers.cbegin()
        end = self.collector_transfers.cend()
        while it != end:
            ct = &deref(it)
            yield t(
                ct.debtor_id,
                ct.from_creditor_id,
                ct.to_creditor_id,
                ct.amount,
            )
            postincrement(it)

    def givings_iter(self):
        """Iterate over the amounts that should be given to the
        buyers.

        Each returned item will be a `AccountChange` namedtuple
        containing four `i64` numbers. The `amount` field specifies
        the amount that will be given, and it will always be a
        positive number.
        """
        if not self.offers_analysis_done:
            self._analyze_offers()

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

    cdef void _analyze_currencies(self):
        cdef double min_trade_amount = float(self.min_trade_amount)
        cdef double price
        self.bid_processor.analyze_bids()

        for debtor_id in self.debtor_ids:
            price = self.bid_processor.get_currency_price(debtor_id)
            if price > 0.0:
                self.graph.add_currency(debtor_id,  min_trade_amount * price)

        self.debtor_ids.clear()
        self.currencies_analysis_done = True

    cdef void _analyze_offers(self):
        cdef tuple t
        cdef double amount
        cdef i64[:] cycle

        while True:
            t = self.graph.find_cycle()
            if t is None:
                break
            amount, cycle = t
            self._process_cycle(amount, cycle)

        self._calc_collector_transfers()
        self.currencies_analysis_done = True
        self.offers_analysis_done = True

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

    cdef void _calc_collector_transfers(self):
        """Generate the list of transfers (`self.collector_transfers`)
        to be performed between collector accounts, so that each
        collector account receives exactly the same amount as it
        gives.
        """
        cdef unordered_multimap[CollectorAccount, i64] collector_amounts
        cdef unordered_set[i64] debtor_ids
        cdef i64 amt, giver_amount, taker_amount
        self.collector_transfers.clear()

        # Gather all "giver" and "taker" collector accounts into an
        # `unordered_multimap`. This multimap is used to efficiently
        # group collector accounts by their `debtor_id`s.
        for pair in self.collection_amounts:
            if pair.second != 0:
                creditor_id = pair.first.creditor_id
                debtor_id = pair.first.debtor_id
                collector_amounts.insert(
                    Pair[CollectorAccount, i64](
                        CollectorAccount(creditor_id, debtor_id),
                        pair.second,
                    )
                )
                debtor_ids.insert(debtor_id)

        for debtor_id in debtor_ids:
            # For each `debtor_id` we create two iterators: one for
            # iterating over "giver" collector accounts (amount > 0),
            # and one for iterating over "taker" collector accounts
            # (amount < 0).
            debtor_root_account = CollectorAccount(0, debtor_id)
            givers = collector_amounts.equal_range(debtor_root_account)
            takers = collector_amounts.equal_range(debtor_root_account)

            # Continue advancing both iterators, and generate
            # transfers from "giver" collector accounts to "taker"
            # collector accounts, until all collector accounts are
            # equalized (amount == 0).
            while givers.first != givers.second:
                if deref(givers.first).second > 0:
                    while takers.first != takers.second:
                        giver_amount = deref(givers.first).second
                        taker_amount = deref(takers.first).second
                        if taker_amount < 0:
                            amt = min(giver_amount, -taker_amount)
                            deref(givers.first).second = giver_amount - amt
                            deref(takers.first).second = taker_amount + amt
                            self.collector_transfers.push_back(
                                Transfer(
                                    debtor_id,
                                    deref(givers.first).first.creditor_id,
                                    deref(takers.first).first.creditor_id,
                                    amt,
                                )
                            )
                            if deref(givers.first).second == 0:
                                break
                            assert deref(takers.first).second == 0
                        postincrement(takers.first)
                    else:
                       raise RuntimeError("can not equalize collectors")

                postincrement(givers.first)

            # Ensure there are no "taker" collectors accounts left.
            while takers.first != takers.second:
                if deref(takers.first).second < 0:
                    raise RuntimeError("can not equalize collectors")
                postincrement(takers.first)

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
