import pytest
from datetime import timedelta, datetime, timezone
from swpt_trade import procedures as p
from swpt_trade.models import (
    Turn,
    DebtorInfo,
    ConfirmedDebtor,
    CurrencyInfo,
    CollectorSending,
    TS0,
)


@pytest.fixture(params=[True, False])
def turn_may_exist(request, db_session):
    if request.param:
        db_session.add(
            Turn(
                started_at=TS0,
                phase=4,
                phase_deadline=TS0,
                collection_started_at=TS0,
                collection_deadline=TS0,
            )
        )
        db_session.commit()

    return request.param


def test_start_new_turn_if_possible(turn_may_exist):
    current_ts = datetime.now(tz=timezone.utc)
    midnight = current_ts.replace(hour=0, minute=0, second=0, microsecond=0)

    # Successfully starts a new turn.
    turns = p.start_new_turn_if_possible(
        turn_period=timedelta(days=1),
        turn_period_offset=current_ts - midnight,
        phase1_duration=timedelta(hours=1),
    )
    assert len(turns) == 1
    assert turns[0].phase == 1
    assert turns[0].phase_deadline is not None
    all_turns = Turn.query.all()
    assert len(all_turns) == 2 if turn_may_exist else 1
    all_turns.sort(key=lambda t: t.phase)
    assert all_turns[0].phase == 1
    assert all_turns[0].phase_deadline is not None

    # Does not start a new turn.
    turns = p.start_new_turn_if_possible(
        turn_period=timedelta(days=1),
        turn_period_offset=current_ts - midnight,
        phase1_duration=timedelta(hours=1),
    )
    assert len(turns) == 1
    assert turns[0].phase == 1
    assert turns[0].phase_deadline is not None
    all_turns = Turn.query.all()
    assert len(all_turns) == 2 if turn_may_exist else 1
    all_turns.sort(key=lambda t: t.phase)
    assert all_turns[0].phase == 1
    assert all_turns[0].phase_deadline is not None


def test_try_to_advance_turn_to_phase2(db_session):
    turn = Turn(phase_deadline=TS0)
    db_session.add(turn)
    db_session.flush()
    db_session.commit()
    turn_id = turn.turn_id

    db_session.add(
        DebtorInfo(
            turn_id=turn_id,
            debtor_info_locator='http://example.com/101',
            debtor_id=101,
            peg_debtor_info_locator=None,
            peg_debtor_id=None,
            peg_exchange_rate=None,
        )
    )
    db_session.add(
        DebtorInfo(
            turn_id=turn_id,
            debtor_info_locator='http://example.com/102',
            debtor_id=102,
            peg_debtor_info_locator='http://example.com/101',
            peg_debtor_id=101,
            peg_exchange_rate=2.0,
        )
    )
    db_session.add(
        ConfirmedDebtor(
            turn_id=turn_id,
            debtor_id=102,
            debtor_info_locator='http://example.com/102',
        )
    )
    db_session.commit()
    assert len(db_session.query(CurrencyInfo).all()) == 0

    # Successful advance.
    p.try_to_advance_turn_to_phase2(
        turn_id=turn_id,
        phase2_duration=timedelta(hours=1),
        max_commit_period=timedelta(days=30),
    )

    currencies = db_session.query(CurrencyInfo).all()
    assert len(currencies) == 2
    currencies.sort(key=lambda c: c.debtor_id)

    assert currencies[0].turn_id == turn_id
    assert currencies[0].debtor_info_locator == 'http://example.com/101'
    assert currencies[0].debtor_id == 101
    assert currencies[0].peg_debtor_info_locator is None
    assert currencies[0].peg_debtor_id is None
    assert currencies[0].peg_exchange_rate is None
    assert currencies[0].is_confirmed is False

    assert currencies[1].turn_id == turn_id
    assert currencies[1].debtor_info_locator == 'http://example.com/102'
    assert currencies[1].debtor_id == 102
    assert currencies[1].peg_debtor_info_locator == 'http://example.com/101'
    assert currencies[1].peg_debtor_id == 101
    assert currencies[1].peg_exchange_rate == 2.0
    assert currencies[1].is_confirmed is True

    all_turns = Turn.query.all()
    assert len(all_turns) == 1
    assert all_turns[0].phase == 2
    assert all_turns[0].phase_deadline is not None
    assert all_turns[0].phase_deadline != TS0

    # Wrong turn_id or phase.
    p.try_to_advance_turn_to_phase2(
        turn_id=-1,
        phase2_duration=timedelta(hours=1),
        max_commit_period=timedelta(days=30),
    )
    p.try_to_advance_turn_to_phase2(
        turn_id=turn_id,
        phase2_duration=timedelta(hours=1),
        max_commit_period=timedelta(days=30),
    )
    all_turns = Turn.query.all()
    assert len(all_turns) == 1
    assert all_turns[0].phase == 2
    assert all_turns[0].phase_deadline is not None
    assert all_turns[0].phase_deadline != TS0


def test_try_to_advance_turn_to_phase4(db_session):
    turn = Turn(
        phase=3,
        phase_deadline=TS0,
        collection_started_at=TS0,
        collection_deadline=TS0,
    )
    db_session.add(turn)
    db_session.flush()
    turn_id = turn.turn_id
    db_session.add(
        CollectorSending(
            turn_id=turn_id,
            debtor_id=102,
            from_collector_id=1,
            to_collector_id=2,
            from_collector_hash=123,
            amount=100000,
        )
    )
    db_session.commit()

    # Can not advance with pending rows.
    p.try_to_advance_turn_to_phase4(turn_id)
    all_turns = Turn.query.all()
    assert len(all_turns) == 1
    assert all_turns[0].phase == 3

    CollectorSending.query.delete()

    # Can advance without pending rows.
    p.try_to_advance_turn_to_phase4(turn_id)
    all_turns = Turn.query.all()
    assert len(all_turns) == 1
    assert all_turns[0].phase == 4
    assert all_turns[0].phase_deadline is None

    # Wrong turn_id or phase.
    p.try_to_advance_turn_to_phase4(-1)
    p.try_to_advance_turn_to_phase4(turn_id)
    all_turns = Turn.query.all()
    assert len(all_turns) == 1
    assert all_turns[0].phase == 4
    assert all_turns[0].phase_deadline is None
