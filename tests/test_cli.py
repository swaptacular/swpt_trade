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
