# distutils: language = c++

cdef i64 MIN_TRADE_AMOUNT = 1000


cdef bool check_if_tradable(i64 debtor_id) noexcept:
    # TODO: Add a real implementation. Debtor IDs that are not
    # tradable must be logged, so as to eventually create an system
    # account for them.
    return True


cdef (i64, float) calc_endorsed_peg(i64 debtor_id) noexcept:
    # TODO: Add a real implementation. Must return the exchange rate
    # to the nearest tradable currency, or the base currency. If there
    # is no exchange rate -- return 0, 0.0.
    return 0, 0.0


cdef void register_tradable_bid(Bid* bid):
    # TODO: Add a real implementation. Each unique `bid.debtor_id`
    # must be recorded, so as to calculate and save the prices for
    # those currencies.
    pass


cdef (i64, float) calc_anchored_peg(Bid* bid) noexcept:
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


cdef bool validate_peg(Bid* bid) noexcept:
    """Compare bid's peg to the endorsed peg.
    """
    id1, rate1 = calc_anchored_peg(bid)
    id2, rate2 = calc_endorsed_peg(bid.debtor_id)
    return id1 == id2 and rate1 == rate2


cdef void process_bid(Bid* bid, i64 base_debtor_id):
    if not bid.processed():
        bid.set_processed()
        tradable = check_if_tradable(bid.debtor_id)

        if bid.debtor_id == base_debtor_id:
            bid.set_anchor()
        else:
            peg_bid = bid.peg_ptr
            if peg_bid == NULL:
                raise RuntimeError("Bid's peg_ptr is NULL.")
            process_bid(peg_bid, base_debtor_id)

            if peg_bid.deadend():
                bid.set_deadend()
            elif tradable:
                if validate_peg(bid):
                    bid.set_anchor()
                else:
                    bid.set_deadend()

        if bid.anchor() and tradable and abs(bid.amount) >= MIN_TRADE_AMOUNT:
            register_tradable_bid(bid)
