# distutils: language = c++
import cython

cdef i64 MIN_TRADE_AMOUNT = 1000
cdef float EPSILON = 1e-5


cdef class BidProcessor:
    def __cinit__(self, i64 base_debtor_id):
        self.base_debtor_id = base_debtor_id
        self.min_trade_amount = MIN_TRADE_AMOUNT
        self.bid_registry_ptr = new BidRegistry(base_debtor_id)
        self.candidate_offers = []

    def __dealloc__(self):
        del self.bid_registry_ptr

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
        candidate_offers = self.candidate_offers

        # Free unused memory.
        del self.bid_registry_ptr
        self.bid_registry_ptr = new BidRegistry(self.base_debtor_id)
        self.candidate_offers = []

        return candidate_offers

    cdef bool _check_if_tradable(self, i64 debtor_id) noexcept:
        # TODO: Add a real implementation. Debtor IDs that are not
        # tradable must be logged, so as to eventually create an
        # system account for them.
        return True

    cdef (i64, float) _calc_endorsed_peg(self, i64 debtor_id) noexcept:
        # TODO: Add a real implementation. Must return the exchange
        # rate to the nearest tradable currency, or the base currency.
        # If there is no exchange rate -- return 0, 0.0.
        return 0, 0.0

    cdef void _add_candidate_offer(self, Bid* bid):
        o = CandidateOffer()
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
    cdef bool _validate_peg(self, Bid* bid) noexcept:
        """Compare bid's peg to the endorsed peg.
        """
        id1, rate1 = self._calc_anchored_peg(bid)
        id2, rate2 = self._calc_endorsed_peg(bid.debtor_id)
        return (
            id1 == id2
            and rate2 != 0.0
            and abs(1.0 - rate1 / rate2) < EPSILON
        )

    cdef void _process_bid(self, Bid* bid) noexcept:
        """If possible, add a candidate offer for the given bid.

        This function assumes that the bids for each trader (aka
        creditor) form a tree, having the `base_debtor_id` as its
        root.
        """
        if not bid.processed():
            bid.set_processed()
            tradable = self._check_if_tradable(bid.debtor_id)

            if bid.debtor_id == self.base_debtor_id:
                bid.set_anchor()
            else:
                peg_bid = bid.peg_ptr
                if peg_bid == NULL:
                    raise RuntimeError("Bid's peg_ptr is NULL.")
                self._process_bid(peg_bid)

                if peg_bid.deadend():
                    bid.set_deadend()
                elif tradable:
                    if self._validate_peg(bid):
                        bid.set_anchor()
                    else:
                        bid.set_deadend()

            if (
                bid.anchor()
                and tradable
                and abs(bid.amount) >= self.min_trade_amount
            ):
                self._add_candidate_offer(bid)
