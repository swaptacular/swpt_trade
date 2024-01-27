from swpt_trade.solve_turn import try_to_advance_turn_to_phase3
from swpt_trade.models import (
    CollectorAccount,
    Turn,
    CurrencyInfo,
    SellOffer,
    BuyOffer,
    TS0,
)


def test_try_to_advance_turn_to_phase3(db_session):
    turn = Turn(
        phase=2,
        phase_deadline=TS0,
        collection_started_at=TS0,
        collection_deadline=TS0,
        base_debtor_info_locator="https://example.com/101",
        base_debtor_id=101,
        max_distance_to_base=5,
        min_trade_amount=5000,
    )
    db_session.add(turn)
    db_session.flush()
    db_session.commit()
    turn_id = turn.turn_id

    db_session.add(
        CurrencyInfo(
            turn_id=turn_id,
            debtor_info_locator='https://example.com/101',
            debtor_id=101,
            peg_debtor_info_locator=None,
            peg_debtor_id=None,
            peg_exchange_rate=None,
            is_confirmed=True,
        )
    )
    db_session.add(
        CurrencyInfo(
            turn_id=turn_id,
            debtor_info_locator='https://example.com/102',
            debtor_id=102,
            peg_debtor_info_locator='https://example.com/101',
            peg_debtor_id=101,
            peg_exchange_rate=2.0,
            is_confirmed=True,
        )
    )
    db_session.add(
        CollectorAccount(debtor_id=101, collector_id=997, account_id="997")
    )
    db_session.add(
        CollectorAccount(debtor_id=101, collector_id=998, account_id="998")
    )
    db_session.add(
        CollectorAccount(debtor_id=102, collector_id=999, account_id="999")
    )
    db_session.add(
        SellOffer(
            turn_id=turn_id,
            creditor_id=1,
            debtor_id=101,
            amount=5000,
            collector_id=997,
        )
    )
    db_session.add(
        SellOffer(
            turn_id=turn_id,
            creditor_id=2,
            debtor_id=101,
            amount=6000,
            collector_id=998,
        )
    )
    db_session.add(
        SellOffer(
            turn_id=turn_id,
            creditor_id=3,
            debtor_id=102,
            amount=10000,
            collector_id=999,
        )
    )
    db_session.add(
        BuyOffer(
            turn_id=turn_id,
            creditor_id=1,
            debtor_id=102,
            amount=20000,
        )
    )
    db_session.add(
        BuyOffer(
            turn_id=turn_id,
            creditor_id=2,
            debtor_id=102,
            amount=20000,
        )
    )
    db_session.add(
        BuyOffer(
            turn_id=turn_id,
            creditor_id=3,
            debtor_id=101,
            amount=30000,
        )
    )
    db_session.commit()

    try_to_advance_turn_to_phase3(turn)
