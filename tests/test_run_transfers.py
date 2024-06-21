from datetime import timedelta
from swpt_trade.run_transfers import process_rescheduled_transfers
from swpt_trade.models import TransferAttempt, TriggerTransferSignal


def test_process_rescheduled_transfers(app, db_session, current_ts):
    ta1 = TransferAttempt(
            collector_id=666,
            turn_id=1,
            debtor_id=222,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1000.5,
            collection_started_at=current_ts,
            recipient="account123",
            recipient_version=1,
            rescheduled_for=current_ts - timedelta(minutes=10),
    )
    ta2 = TransferAttempt(
            collector_id=666,
            turn_id=1,
            debtor_id=333,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1000.5,
            collection_started_at=current_ts,
            recipient="account123",
            recipient_version=1,
            rescheduled_for=None,
    )
    ta3 = TransferAttempt(
            collector_id=666,
            turn_id=1,
            debtor_id=444,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1000.5,
            collection_started_at=current_ts,
            recipient="account123",
            recipient_version=1,
            rescheduled_for=current_ts + timedelta(days=10),
    )
    db_session.add(ta1)
    db_session.add(ta2)
    db_session.add(ta3)
    db_session.commit()

    process_rescheduled_transfers()
    attempts = TransferAttempt.query.all()
    attempts.sort(key=lambda x: x.debtor_id)
    assert len(attempts) == 3
    assert attempts[0].debtor_id == 222
    assert attempts[0].rescheduled_for is None
    assert attempts[1].debtor_id == 333
    assert attempts[1].rescheduled_for is None
    assert attempts[2].debtor_id == 444
    assert attempts[2].rescheduled_for is not None

    tts = TriggerTransferSignal.query.all()
    assert len(tts) == 1
    assert tts[0].collector_id == 666
    assert tts[0].turn_id == 1
    assert tts[0].debtor_id == 222
    assert tts[0].creditor_id == 123
    assert tts[0].is_dispatching is True
