# distutils: language = c++
import hashlib
from cpython cimport array
from libcpp cimport bool

cdef i64 DEFAULT_MIN_TRADE_AMOUNT = 1000
cdef distance DEFAULT_MAX_DISTANCE_TO_BASE = 10


cdef class CandidateOffer:
    def is_buy_offer(self):
        return self.amount > 0

    def is_sell_offer(self):
        return self.amount < 0


cdef class BidProcessor:
    def __cinit__(
        self,
        str base_debtor_info_uri,
        i64 base_debtor_id,
        distance max_distance_to_base=DEFAULT_MAX_DISTANCE_TO_BASE,
        i64 min_trade_amount=DEFAULT_MIN_TRADE_AMOUNT,
    ):
        self.base_debtor_info_uri = base_debtor_info_uri
        self.base_debtor_id = base_debtor_id
        self.max_distance_to_base = max_distance_to_base
        self.min_trade_amount = min_trade_amount
        self.bid_registry_ptr = new BidRegistry(base_debtor_id)
        self.peg_registry_ptr = new PegRegistry(
            self._calc_key128(base_debtor_info_uri),
            base_debtor_id,
            max_distance_to_base,
        )
        self.candidate_offers = []

    def __dealloc__(self):
        del self.bid_registry_ptr
        del self.peg_registry_ptr

    def register_currency(
        self,
        bool confirmed,
        str debtor_info_uri,
        i64 debtor_id,
        str peg_debtor_info_uri='',
        i64 peg_debtor_id=0,
        float peg_exchange_rate=0.0,
    ):
        self.peg_registry_ptr.add_currency(
            self._calc_key128(debtor_info_uri),
            debtor_id,
            self._calc_key128(peg_debtor_info_uri),
            peg_debtor_id,
            peg_exchange_rate,
            confirmed,
        )

    def get_currency_price(self, i64 debtor_id):
        peg_registry = self.peg_registry_ptr
        peg_registry.prepare_for_queries()
        return peg_registry.get_currency_price(debtor_id)

    def register_bid(
        self,
        i64 creditor_id,
        i64 debtor_id,
        i64 amount,
        i64 peg_debtor_id=0,
        float peg_exchange_rate=0.0,
    ):
        self.bid_registry_ptr.add_bid(
            creditor_id,
            debtor_id,
            amount,
            peg_debtor_id,
            peg_exchange_rate,
        )

    def generate_candidate_offers(self):
        self.peg_registry_ptr.prepare_for_queries()
        bid_registry = self.bid_registry_ptr

        while (bid := bid_registry.get_priceable_bid()) != NULL:
            currency = self._find_tradable_currency(bid)
            if (
                currency != NULL
                and compare_prices(bid.currency_price, currency.currency_price)
                and abs(bid.amount) >= self.min_trade_amount
            ):
                self._add_candidate_offer(bid)

        # Obviously, no deals can be arranged for traders which do not
        # have at least one buy offer, and at least one sell offer.
        # Therefore, we eliminate offers from such traders.
        cdef CandidateOffer o
        candidate_offers = [
            o for o in self.candidate_offers if (
                self.buyers.count(o.creditor_id)
                if o.amount < 0
                else self.sellers.count(o.creditor_id)
            ) != 0
        ]

        # Free unused memory.
        del bid_registry
        self.bid_registry_ptr = new BidRegistry(self.base_debtor_id)
        self.candidate_offers.clear()
        self.buyers.clear()
        self.sellers.clear()

        return candidate_offers

    def currencies_to_be_confirmed(self):
        for debtor_id in self.to_be_confirmed:
            yield debtor_id

    cdef Key128 _calc_key128(self, str uri):
        m = hashlib.sha256()
        m.update(uri.encode('utf8'))
        cdef i64[:] data = array.array('q', m.digest())
        return Key128(data[0], data[1])

    cdef Peg* _find_tradable_currency(self, Bid* bid):
        currency = self.peg_registry_ptr.get_tradable_currency(bid.debtor_id)
        if currency == NULL and bid.amount <= -self.min_trade_amount:
            # We must try to create a system account for this
            # currency, so that it can be traded in the future.
            self.to_be_confirmed.insert(bid.debtor_id)

        return currency

    cdef void _add_candidate_offer(self, Bid* bid):
        if bid.amount > 0:
            self.buyers.insert(bid.creditor_id)
        else:
            self.sellers.insert(bid.creditor_id)

        cdef CandidateOffer o = CandidateOffer()
        o.amount = bid.amount
        o.debtor_id = bid.debtor_id
        o.creditor_id = bid.creditor_id
        self.candidate_offers.append(o)
