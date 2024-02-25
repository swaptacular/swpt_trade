from datetime import datetime, timezone
from unittest.mock import Mock
from swpt_trade.extensions import db
from swpt_trade import models as m

D_ID = -1
C_ID = 4294967296


def test_consume_messages(app):
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=["swpt_trade", "consume_messages", "--url=INVALID"]
    )
    assert result.exit_code == 1


def test_consume_chore_messages(app):
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=["swpt_trade", "consume_chore_messages", "--url=INVALID"]
    )
    assert result.exit_code == 1


def test_flush_messages(mocker, app, db_session):
    send_signalbus_message = Mock()
    mocker.patch(
        "swpt_trade.models.FinalizeTransferSignal.send_signalbus_message",
        new_callable=send_signalbus_message,
    )
    fts = m.FinalizeTransferSignal(
        creditor_id=0x0000010000000000,
        debtor_id=D_ID,
        transfer_id=666,
        coordinator_id=C_ID,
        coordinator_request_id=777,
        committed_amount=0,
        transfer_note_format="",
        transfer_note="",
    )
    db.session.add(fts)
    db.session.commit()
    assert len(m.FinalizeTransferSignal.query.all()) == 1
    db.session.commit()

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "flush_messages",
            "FinalizeTransferSignal",
            "--wait",
            "0.1",
            "--quit-early",
        ]
    )
    assert result.exit_code == 1
    assert send_signalbus_message.called_once()
    assert len(m.FinalizeTransferSignal.query.all()) == 0


def test_roll_turns(app, db_session):
    current_ts = datetime.now(tz=timezone.utc)
    midnight = current_ts.replace(hour=0, minute=0, second=0, microsecond=0)
    offset_seconds = (current_ts - midnight).total_seconds()
    runner = app.test_cli_runner()

    def invoke():
        return runner.invoke(
            args=[
                "swpt_trade",
                "roll_turns",
                "--period=24h",
                f"--period-offset={offset_seconds}",
                "--quit-early",
            ]
        )

    result = invoke()
    assert result.exit_code == 0
    turns = m.Turn.query.all()
    assert len(turns) == 1
    assert turns[0].phase == 1

    result = invoke()
    assert result.exit_code == 0
    turns = m.Turn.query.all()
    assert len(turns) == 1
    assert turns[0].phase == 2

    result = invoke()
    assert result.exit_code == 0
    turns = m.Turn.query.all()
    assert len(turns) == 1
    assert turns[0].phase == 3

    result = invoke()
    assert result.exit_code == 0
    turns = m.Turn.query.all()
    assert len(turns) == 1
    assert turns[0].phase == 4

    result = invoke()
    assert result.exit_code == 0
    turns = m.Turn.query.all()
    assert len(turns) == 1
    assert turns[0].phase == 4


def test_fetch_debtor_infos(mocker, app, db_session):
    from swpt_trade.fetch_debtor_infos import FetchResult

    def perform_fetches(fetches, **kwargs):
        return [
            FetchResult(
                fetch=f,
                document=m.DebtorInfoDocument(
                    debtor_info_locator=f.iri,
                    debtor_id=f.debtor_id,
                    peg_debtor_info_locator="https://example.com/777",
                    peg_debtor_id=777,
                    peg_exchange_rate=2.0,
                ),
                store_document=f.is_locator_fetch,
            ) for f in fetches
        ]

    mocker.patch(
        "swpt_trade.fetch_debtor_infos._perform_fetches",
        new=perform_fetches,
    )

    dif = m.DebtorInfoFetch(
        iri="https://example.com/666",
        debtor_id=666,
        is_locator_fetch=True,
        is_discovery_fetch=True,
        ignore_cache=True,
    )
    db.session.add(dif)
    db.session.commit()
    assert len(m.DebtorInfoFetch.query.all()) == 1
    assert len(m.FetchDebtorInfoSignal.query.all()) == 0
    db.session.commit()

    runner = app.test_cli_runner()

    def invoke():
        return runner.invoke(
            args=[
                "swpt_trade",
                "fetch_debtor_infos",
                "--timeout", "0.1",
                "--quit-early",
            ]
        )

    result = invoke()
    assert result.exit_code == 1

    assert len(m.DebtorInfoFetch.query.all()) == 0
    assert len(m.DiscoverDebtorSignal.query.all()) == 0
    assert len(m.DebtorLocatorClaim.query.all()) == 0
    assert len(m.DebtorInfoDocument.query.all()) == 0

    fetch_signals = m.FetchDebtorInfoSignal.query.all()
    assert len(fetch_signals) == 1
    assert fetch_signals[0].iri == "https://example.com/777"
    assert fetch_signals[0].debtor_id == 777
    assert fetch_signals[0].is_locator_fetch is True
    assert fetch_signals[0].is_discovery_fetch is False
    assert fetch_signals[0].ignore_cache is False
    assert fetch_signals[0].recursion_level == 1

    stored_signals = m.StoreDocumentSignal.query.all()
    assert len(stored_signals) == 1
    assert stored_signals[0].debtor_info_locator == "https://example.com/666"
    assert stored_signals[0].debtor_id == 666
    assert stored_signals[0].peg_debtor_info_locator == (
        "https://example.com/777"
    )
    assert stored_signals[0].peg_debtor_id == 777
    assert stored_signals[0].peg_exchange_rate == 2.0
    assert stored_signals[0].will_not_change_until is None

    confirmations = m.ConfirmDebtorSignal.query.all()
    assert len(confirmations) == 1
    assert confirmations[0].debtor_id == 666
    assert confirmations[0].debtor_info_locator == "https://example.com/666"
