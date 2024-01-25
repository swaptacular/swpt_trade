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
