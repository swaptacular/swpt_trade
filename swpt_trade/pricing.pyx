# distutils: language = c++
import cython
import hashlib
from cpython cimport array
from libcpp cimport bool

cdef i64 MIN_TRADE_AMOUNT = 1000
cdef float EPSILON = 1e-5
cdef distance DEFAULT_MAX_DISTANCE_TO_BASE = 10


cdef class BidProcessor:
    def __cinit__(
        self,
        str base_debtor_uri,
        i64 base_debtor_id,
        distance max_distance_to_base=DEFAULT_MAX_DISTANCE_TO_BASE,
    ):
        self.base_debtor_uri = base_debtor_uri
        self.base_debtor_id = base_debtor_id
        self.max_distance_to_base = max_distance_to_base
        self.min_trade_amount = MIN_TRADE_AMOUNT
        self.bid_registry_ptr = new BidRegistry(base_debtor_id)
        self.peg_registry_ptr = new PegRegistry(
            self._calc_key128(base_debtor_uri),
            base_debtor_id,
            max_distance_to_base,
        )
        self.candidate_offers = []

    def __dealloc__(self):
        del self.bid_registry_ptr
        del self.peg_registry_ptr

    def register_bid(
        self,
        i64 creditor_id,
        i64 debtor_id,
        i64 amount,
        object peg_debtor_id=None,
        object peg_exchange_rate=None,
    ):
        if peg_debtor_id is None:
            peg_debtor_id = 0
        if peg_exchange_rate is None:
            peg_exchange_rate = 0.0

        self.bid_registry_ptr.add_bid(
            creditor_id,
            debtor_id,
            amount,
            peg_debtor_id,
            peg_exchange_rate,
        )

    def generate_candidate_offers(self):
        r = self.bid_registry_ptr

        while (bid := r.get_priceable_bid()) != NULL:
            self._process_bid(bid)

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
        del self.bid_registry_ptr
        self.bid_registry_ptr = new BidRegistry(self.base_debtor_id)
        self.candidate_offers = []
        self.buyers.clear()
        self.sellers.clear()

        return candidate_offers

    cdef Key128 _calc_key128(self, str uri):
        m = hashlib.sha256()
        m.update(uri.encode('utf8'))
        cdef i64[:] data = array.array('q', m.digest())
        return Key128(data[0], data[1])

    cdef Peg* _find_tradable_peg(self, Bid* bid) noexcept:
        # TODO: Add a real implementation. `bid`s for non-tradable
        # currencies, for which `bid.amount <= -self.min_trade_amount`,
        # must be logged, so as to eventually create system accounts for them.
        return NULL

    cdef (i64, float) _calc_endorsed_peg(self, Peg* tradable_peg) noexcept:
        # TODO: Add a real implementation. Must return the exchange
        # rate to the nearest tradable currency, or the base currency.
        # If there is no exchange rate -- return 0, 0.0.
        return 0, 0.0

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

    cdef (i64, float) _calc_anchored_peg(self, Bid* bid) noexcept:
        """Try to calculate the exchange rate to the nearest anchor-currency.

        If there are no anchor-currencies in the peg-chain, returns the
        exchange rate to the last currency in the peg-chain. May enter an
        infinite cycle if there is a cycle in the peg-chain.
        """
        cdef i64 anchor_debtor_id = bid.debtor_id
        cdef float exchange_rate = 1.0

        while not bid.anchor() and bid.peg_ptr != NULL:
            exchange_rate *= bid.peg_exchange_rate
            bid = bid.peg_ptr
            anchor_debtor_id = bid.debtor_id

        return anchor_debtor_id, exchange_rate

    @cython.cdivision(True)
    cdef bool _validate_peg(self, Bid* bid, Peg* tradable_peg) noexcept:
        """Compare bid's peg to the endorsed peg.
        """
        id1, rate1 = self._calc_anchored_peg(bid)
        id2, rate2 = self._calc_endorsed_peg(tradable_peg)
        return (
            id1 == id2
            and rate2 != 0.0
            and abs(1.0 - rate1 / rate2) < EPSILON
        )

    cdef void _process_bid(self, Bid* bid):
        """If possible, add a candidate offer for the given bid.

        This function assumes that the bids for each trader (aka
        creditor) form a tree, having the `base_debtor_id` as its
        root.
        """
        cdef bool is_tradable

        if not bid.processed():
            bid.set_processed()
            tradable_peg = self._find_tradable_peg(bid)
            is_tradable = tradable_peg != NULL

            if bid.debtor_id == self.base_debtor_id:
                bid.set_anchor()
            else:
                peg_bid = bid.peg_ptr
                if peg_bid == NULL:
                    raise RuntimeError("Bid's peg_ptr is NULL.")
                self._process_bid(peg_bid)

                if peg_bid.deadend():
                    bid.set_deadend()
                elif is_tradable:
                    if self._validate_peg(bid, tradable_peg):
                        bid.set_anchor()
                    else:
                        bid.set_deadend()

            if (
                bid.anchor()
                and is_tradable
                and abs(bid.amount) >= self.min_trade_amount
            ):
                self._add_candidate_offer(bid)
