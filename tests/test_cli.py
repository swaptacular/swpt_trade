import sqlalchemy
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock
from swpt_pythonlib.utils import ShardingRealm
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

    def make_https_requests(fetches, **kwargs):
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
        "swpt_trade.fetch_debtor_infos._make_https_requests",
        new=make_https_requests,
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


def test_delete_parent_documents(app, db_session, restore_sharding_realm):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    d1 = m.DebtorInfoDocument(
        debtor_info_locator="https://example.com/666",
        debtor_id=666,
    )
    d2 = m.DebtorInfoDocument(
        debtor_info_locator="https://example.com/888",
        debtor_id=888,
    )
    d3 = m.DebtorInfoDocument(
        debtor_info_locator="https://example.com/999",
        debtor_id=999,
    )
    db.session.add(d1)
    db.session.add(d2)
    db.session.add(d3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE debtor_info_document"))

    assert len(m.DebtorInfoDocument.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_debtor_info_documents",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    documents = m.DebtorInfoDocument.query.all()
    assert len(documents) == 1
    assert documents[0].debtor_info_locator == "https://example.com/666"


def test_delete_stale_documents(app, db_session, current_ts):
    d1 = m.DebtorInfoDocument(
        debtor_info_locator="https://example.com/666",
        debtor_id=666,
        fetched_at=current_ts - timedelta(days=10000),
    )
    d2 = m.DebtorInfoDocument(
        debtor_info_locator="https://example.com/888",
        debtor_id=888,
    )
    db.session.add(d1)
    db.session.add(d2)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE debtor_info_document"))

    assert len(m.DebtorInfoDocument.query.all()) == 2
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_debtor_info_documents",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    documents = m.DebtorInfoDocument.query.all()
    assert len(documents) == 1
    assert documents[0].debtor_info_locator == "https://example.com/888"


def test_delete_parent_claims(app, db_session, restore_sharding_realm):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    c1 = m.DebtorLocatorClaim(debtor_id=666)
    c2 = m.DebtorLocatorClaim(debtor_id=777)
    c3 = m.DebtorLocatorClaim(debtor_id=888)
    db.session.add(c1)
    db.session.add(c2)
    db.session.add(c3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE debtor_locator_claim"))

    assert len(m.DebtorLocatorClaim.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_debtor_locator_claims",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    claims = m.DebtorLocatorClaim.query.all()
    assert len(claims) == 1
    assert claims[0].debtor_id == 888


def test_delete_stale_claims(app, db_session, current_ts):
    c1 = m.DebtorLocatorClaim(debtor_id=666)
    c2 = m.DebtorLocatorClaim(
        debtor_id=777,
        latest_discovery_fetch_at=current_ts - timedelta(days=10),
    )
    c3 = m.DebtorLocatorClaim(
        debtor_id=888,
        debtor_info_locator="https://example.com/888",
        latest_locator_fetch_at=current_ts - timedelta(days=100),
    )
    db.session.add(c1)
    db.session.add(c2)
    db.session.add(c3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE debtor_info_document"))

    assert len(m.DebtorLocatorClaim.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_debtor_locator_claims",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    claims = m.DebtorLocatorClaim.query.all()
    assert len(claims) == 1
    assert claims[0].debtor_id == 666


def test_delete_parent_trading_policies(
        app,
        db_session,
        restore_sharding_realm,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    tp1 = m.TradingPolicy(creditor_id=666, debtor_id=1, account_id='row1')
    tp2 = m.TradingPolicy(creditor_id=777, debtor_id=2, account_id='row2')
    tp3 = m.TradingPolicy(creditor_id=888, debtor_id=2, account_id='row3')
    db.session.add(tp1)
    db.session.add(tp2)
    db.session.add(tp3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE trading_policy"))

    assert len(m.TradingPolicy.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_trading_policies",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    tps = m.TradingPolicy.query.all()
    assert len(tps) == 1
    assert tps[0].creditor_id == 888


def test_delete_useless_trading_policies(app, db_session, current_ts):
    tp1 = m.TradingPolicy(creditor_id=666, debtor_id=1)
    tp2 = m.TradingPolicy(creditor_id=777, debtor_id=2, account_id='test')
    tp3 = m.TradingPolicy(creditor_id=888, debtor_id=2)
    db.session.add(tp1)
    db.session.add(tp2)
    db.session.add(tp3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE debtor_info_document"))

    assert len(m.TradingPolicy.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_trading_policies",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    tps = m.TradingPolicy.query.all()
    assert len(tps) == 1
    assert tps[0].creditor_id == 777


def test_delete_parent_worker_accounts(
        app,
        db_session,
        restore_sharding_realm,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    params = {
        "creation_date": m.DATE0,
        "last_change_ts": m.TS0,
        "last_change_seqnum": 1,
        "principal": 100,
        "interest": 31.4,
        "interest_rate": 5.0,
        "last_interest_rate_change_ts": m.TS0,
        "config_flags": 0,
        "account_id": "Account123",
        "last_transfer_number": 2,
        "last_transfer_committed_at": m.TS0,
        "demurrage_rate": -50.0,
        "commit_period": 1000000,
        "transfer_note_max_bytes": 500,
        "debtor_info_iri": "https://example.com/666",
    }
    wa1 = m.WorkerAccount(creditor_id=666, debtor_id=1, **params)
    wa2 = m.WorkerAccount(creditor_id=777, debtor_id=2, **params)
    wa3 = m.WorkerAccount(creditor_id=888, debtor_id=2, **params)
    db.session.add(wa1)
    db.session.add(wa2)
    db.session.add(wa3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE worker_account"))

    assert len(m.WorkerAccount.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_worker_accounts",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    was = m.WorkerAccount.query.all()
    assert len(was) == 1
    assert was[0].creditor_id == 888


def test_delete_dead_worker_accounts(app, db_session, current_ts):
    params = {
        "debtor_id": 666,
        "creation_date": m.DATE0,
        "last_change_ts": m.TS0,
        "last_change_seqnum": 1,
        "principal": 100,
        "interest": 31.4,
        "interest_rate": 5.0,
        "last_interest_rate_change_ts": m.TS0,
        "config_flags": 0,
        "account_id": "Account123",
        "last_transfer_number": 2,
        "last_transfer_committed_at": m.TS0,
        "demurrage_rate": -50.0,
        "commit_period": 1000000,
        "transfer_note_max_bytes": 500,
        "debtor_info_iri": "https://example.com/666",
    }
    wa1 = m.WorkerAccount(creditor_id=666, **params)
    wa2 = m.WorkerAccount(
        creditor_id=777,
        last_heartbeat_ts=current_ts - timedelta(days=100000),
        **params
    )
    db.session.add(wa1)
    db.session.add(wa2)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE worker_account"))

    assert len(m.WorkerAccount.query.all()) == 2
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_worker_accounts",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    was = m.WorkerAccount.query.all()
    assert len(was) == 1
    assert was[0].creditor_id == 666


def test_delete_parent_needed_worker_accounts(
        app,
        db_session,
        restore_sharding_realm,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    nwa1 = m.NeededWorkerAccount(creditor_id=666, debtor_id=1)
    nwa2 = m.NeededWorkerAccount(creditor_id=777, debtor_id=2)
    nwa3 = m.NeededWorkerAccount(creditor_id=888, debtor_id=2)
    db.session.add(nwa1)
    db.session.add(nwa2)
    db.session.add(nwa3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE needed_worker_account"))

    assert len(m.NeededWorkerAccount.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_needed_worker_accounts",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    nwas = m.NeededWorkerAccount.query.all()
    assert len(nwas) == 1
    assert nwas[0].creditor_id == 888


def test_process_pristine_collectors(
        app,
        db_session,
        restore_sharding_realm,
        current_ts,
):
    app.config["SHARDING_REALM"] = sr = ShardingRealm("1.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = False

    assert sr.match(123)
    assert sr.match(127)
    assert sr.match(128)
    assert not sr.match(129)

    ca1 = m.CollectorAccount(debtor_id=666, collector_id=123, status=0)
    ca2 = m.CollectorAccount(debtor_id=666, collector_id=127, status=0)
    nwa2 = m.NeededWorkerAccount(creditor_id=127, debtor_id=666)
    wa2 = m.WorkerAccount(
        creditor_id=127,
        debtor_id=666,
        creation_date=m.DATE0,
        last_change_ts=current_ts,
        last_change_seqnum=1,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_interest_rate_change_ts=m.TS0,
        config_flags=0,
        account_id="Account127",
        last_transfer_number=0,
        last_transfer_committed_at=current_ts,
        demurrage_rate=-50.0,
        commit_period=1000000,
        transfer_note_max_bytes=500,
        last_heartbeat_ts=current_ts,
    )
    ca3 = m.CollectorAccount(debtor_id=666, collector_id=128, status=0)
    nwa3 = m.NeededWorkerAccount(
        creditor_id=128, debtor_id=666, configured_at=m.TS0
    )

    # Collector account from another shard (will be ignored).
    ca4 = m.CollectorAccount(debtor_id=666, collector_id=129, status=0)

    db.session.add(ca1)
    db.session.add(ca2)
    db.session.add(nwa2)
    db.session.add(wa2)
    db.session.add(ca3)
    db.session.add(nwa3)
    db.session.commit()
    db.session.add(ca4)

    assert len(m.CollectorAccount.query.all()) == 4
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "process_pristine_collectors",
            "--wait",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    cas = m.CollectorAccount.query.all()
    cas.sort(key=lambda x: (x.debtor_id, x.collector_id))
    assert len(cas) == 4
    assert cas[0].status == 1
    assert cas[0].account_id == ""
    assert cas[1].status == 1
    assert cas[1].account_id == ""
    assert cas[2].status == 1
    assert cas[2].account_id == ""
    assert cas[3].status == 0
    assert cas[3].account_id == ""

    ca_signals = m.ConfigureAccountSignal.query.all()
    ca_signals.sort(key=lambda x: (x.debtor_id, x.creditor_id))
    assert len(ca_signals) == 2
    assert ca_signals[0].creditor_id == 123
    assert ca_signals[0].debtor_id == 666
    assert ca_signals[0].ts >= current_ts
    assert ca_signals[0].negligible_amount == 1e30
    assert ca_signals[0].config_data == ""
    assert ca_signals[0].config_flags == 0
    assert ca_signals[1].creditor_id == 128
    assert ca_signals[1].debtor_id == 666
    assert ca_signals[1].ts >= current_ts
    assert ca_signals[1].negligible_amount == 1e30
    assert ca_signals[1].config_data == ""
    assert ca_signals[1].config_flags == 0


def test_update_worker_turns(app, db_session, current_ts):
    t1 = m.Turn(
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=2,
        phase_deadline=current_ts - timedelta(days=100),
        collection_deadline=current_ts - timedelta(days=50),
    )
    t2 = m.Turn(
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=1,
        phase_deadline=current_ts - timedelta(days=99.1),
        collection_deadline=current_ts - timedelta(days=49),
    )
    db.session.add(t1)
    db.session.add(t2)
    db.session.flush()

    wt1 = m.WorkerTurn(
        turn_id=t1.turn_id,
        started_at=t1.started_at,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=1,
        phase_deadline=current_ts - timedelta(days=100.1),
        worker_turn_subphase=5,
    )
    db.session.add(wt1)
    db.session.commit()

    assert len(m.WorkerTurn.query.all()) == 1
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "roll_worker_turns",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    wts = m.WorkerTurn.query.all()
    wts.sort(key=lambda t: t.phase, reverse=True)
    assert len(wts) == 2
    assert wts[0].turn_id == t1.turn_id
    assert wts[0].started_at == t1.started_at
    assert wts[0].base_debtor_info_locator == t1.base_debtor_info_locator
    assert wts[0].base_debtor_id == t1.base_debtor_id
    assert wts[0].max_distance_to_base == t1.max_distance_to_base
    assert wts[0].min_trade_amount == t1.min_trade_amount
    assert wts[0].phase == t1.phase
    assert wts[0].phase_deadline == t1.phase_deadline
    assert wts[0].collection_started_at == t1.collection_started_at
    assert wts[0].collection_deadline == t1.collection_deadline
    assert wts[0].worker_turn_subphase == 0
    assert wts[1].turn_id == t2.turn_id
    assert wts[1].started_at == t2.started_at
    assert wts[1].base_debtor_info_locator == t2.base_debtor_info_locator
    assert wts[1].base_debtor_id == t2.base_debtor_id
    assert wts[1].max_distance_to_base == t2.max_distance_to_base
    assert wts[1].min_trade_amount == t2.min_trade_amount
    assert wts[1].phase == t2.phase
    assert wts[1].phase_deadline == t2.phase_deadline
    assert wts[1].collection_started_at == t2.collection_started_at
    assert wts[1].collection_deadline == t2.collection_deadline
    assert wts[1].worker_turn_subphase == 0
