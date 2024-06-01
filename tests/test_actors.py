import pytest
from datetime import datetime, timezone, date
from swpt_pythonlib.utils import ShardingRealm
from swpt_pythonlib.rabbitmq import MessageProperties
from swpt_trade import models as m

D_ID = -1
C_ID = 1


@pytest.fixture(scope="function")
def actors():
    from swpt_trade import actors

    return actors


def test_on_rejected_config_signal(db_session, actors):
    actors._on_rejected_config_signal(
        debtor_id=D_ID,
        creditor_id=m.ROOT_CREDITOR_ID,
        config_ts=datetime.fromisoformat("2019-10-01T00:00:00+00:00"),
        config_seqnum=123,
        negligible_amount=m.HUGE_NEGLIGIBLE_AMOUNT,
        config_data="",
        config_flags=0,
        rejection_code="TEST_REJECTION",
        ts=datetime.fromisoformat("2019-10-01T00:00:00+00:00"),
    )


def test_on_account_update_signal(db_session, actors):
    actors._on_account_update_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        last_change_seqnum=0,
        last_change_ts=datetime.fromisoformat("2019-10-01T00:00:00+00:00"),
        principal=1000,
        interest=0.0,
        interest_rate=-0.5,
        last_interest_rate_change_ts=datetime.fromisoformat(
            "2019-09-01T00:00:00+00:00"
        ),
        last_config_ts=datetime.fromisoformat("1970-01-01T00:00:00+00:00"),
        last_config_seqnum=0,
        creation_date=date.fromisoformat("2018-10-01"),
        negligible_amount=2.0,
        config_data="",
        config_flags=0,
        account_id="0",
        demurrage_rate=-40.0,
        commit_period=10000000,
        transfer_note_max_bytes=500,
        last_transfer_number=123,
        last_transfer_committed_at=datetime.fromisoformat(
            "2019-08-01T00:00:00+00:00"
        ),
        debtor_info_iri="https://example.com/test",
        ts=datetime.now(tz=timezone.utc),
        ttl=1000000,
    )


def test_on_prepared_agent_transfer_signal(db_session, actors):
    actors._on_prepared_agent_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_id=123,
        coordinator_type="agent",
        coordinator_id=C_ID,
        coordinator_request_id=456,
        locked_amount=1000,
        recipient=str(C_ID),
        prepared_at=datetime.fromisoformat("2019-10-01T00:00:00+00:00"),
        demurrage_rate=-50,
        deadline=datetime.now(tz=timezone.utc),
        final_interest_rate_ts=datetime.now(tz=timezone.utc),
        ts=datetime.now(tz=timezone.utc),
    )
    fts = m.FinalizeTransferSignal.query.one()
    assert fts.creditor_id == C_ID
    assert fts.debtor_id == D_ID
    assert fts.coordinator_id == C_ID
    assert fts.coordinator_request_id == 456
    assert fts.transfer_id == 123
    assert fts.committed_amount == 0
    assert fts.transfer_note_format == ""
    assert fts.transfer_note == ""


def test_on_rejected_agent_transfer_signal(db_session, actors):
    actors._on_rejected_agent_transfer_signal(
        coordinator_type="agent",
        coordinator_id=C_ID,
        coordinator_request_id=1,
        status_code="TEST",
        total_locked_amount=0,
        debtor_id=D_ID,
        creditor_id=m.ROOT_CREDITOR_ID,
        ts=datetime.now(tz=timezone.utc),
    )


@pytest.mark.skip
def test_on_finalized_agent_transfer_signal(db_session, actors):
    actors._on_finalized_agent_transfer_signal(
        debtor_id=D_ID,
        creditor_id=m.ROOT_CREDITOR_ID,
        transfer_id=123,
        coordinator_type="agent",
        coordinator_id=D_ID,
        coordinator_request_id=678,
        recipient="1235",
        prepared_at=datetime.fromisoformat("2019-10-01T00:00:00+00:00"),
        ts=datetime.fromisoformat("2019-10-01T00:00:00+00:00"),
        committed_amount=100,
        status_code="OK",
        total_locked_amount=0,
    )


def test_on_updated_ledger_signal(db_session, actors):
    actors._on_updated_ledger_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        update_id=123,
        account_id="test_account",
        creation_date=date.fromisoformat("2018-10-01"),
        principal=250000,
        last_transfer_number=123,
        ts=datetime.now(tz=timezone.utc),
    )


def test_on_updated_policy_signal(db_session, actors):
    actors._on_updated_policy_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        update_id=123,
        policy_name="conservative",
        min_principal=0,
        max_principal=1000000,
        peg_exchange_rate=2.0,
        peg_debtor_id=666,
        ts=datetime.now(tz=timezone.utc),
    )


def test_on_updated_flags_signal(db_session, actors):
    actors._on_updated_flags_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        update_id=123,
        config_flags=0b100,
        ts=datetime.now(tz=timezone.utc),
    )


def test_on_fetch_debtor_info_signal(db_session, actors):
    actors._on_fetch_debtor_info_signal(
        iri="https://example.com/test",
        debtor_id=D_ID,
        is_locator_fetch=True,
        is_discovery_fetch=False,
        ignore_cache=False,
        recursion_level=2,
        ts=datetime.now(tz=timezone.utc),
    )


def test_on_store_document_signal(db_session, actors):
    actors._on_store_document_signal(
        debtor_info_locator="https://example.com/test",
        debtor_id=D_ID,
        peg_debtor_info_locator="https://example.com/peg",
        peg_debtor_id=666,
        peg_exchange_rate=3.14,
        will_not_change_until=datetime.now(tz=timezone.utc),
        ts=datetime.now(tz=timezone.utc),
    )


def test_on_discover_debtor_signal(db_session, actors):
    actors._on_discover_debtor_signal(
        debtor_id=D_ID,
        iri="https://example.com/test",
        force_locator_refetch=False,
        ts=datetime.now(tz=timezone.utc),
    )


def test_on_confirm_debtor_signal(db_session, actors):
    actors._on_confirm_debtor_signal(
        debtor_id=D_ID,
        debtor_info_locator="https://example.com/test",
        ts=datetime.now(tz=timezone.utc),
    )


def test_on_activate_collector_signal(db_session, actors):
    actors._on_activate_collector_signal(
        debtor_id=D_ID,
        creditor_id=m.ROOT_CREDITOR_ID,
        account_id='test_account',
        ts=datetime.now(tz=timezone.utc),
    )


def test_on_candidate_offer_signal(db_session, actors):
    actors._on_candidate_offer_signal(
        turn_id=5,
        debtor_id=D_ID,
        creditor_id=m.ROOT_CREDITOR_ID,
        amount=10000,
        account_creation_date=date(2024, 3, 10),
        last_transfer_number=1234,
        ts=datetime.now(tz=timezone.utc),
    )


def test_on_needed_collector_signal(db_session, actors):
    actors._on_needed_collector_signal(
        debtor_id=D_ID,
        ts=datetime.now(tz=timezone.utc),
    )


def test_on_revise_account_lock_signal(db_session, actors):
    actors._on_revise_account_lock_signal(
        creditor_id=C_ID,
        debtor_id=D_ID,
        turn_id=1,
        ts=datetime.now(tz=timezone.utc),
    )


def test_consumer(db_session, app, actors, restore_sharding_realm):
    consumer = actors.SmpConsumer()

    props = MessageProperties(content_type="xxx")
    assert consumer.process_message(b"body", props) is False

    props = MessageProperties(content_type="application/json", type="xxx")
    assert consumer.process_message(b"body", props) is False

    props = MessageProperties(
        content_type="application/json", type="AccountPurge"
    )
    assert consumer.process_message(b"body", props) is False

    props = MessageProperties(
        content_type="application/json", type="AccountPurge"
    )
    assert consumer.process_message(b"{}", props) is False

    props = MessageProperties(
        content_type="application/json", type="AccountPurge"
    )

    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = False
    with pytest.raises(
        RuntimeError, match="The server is not responsible for this shard."
    ):
        consumer.process_message(
            b"""
        {
          "type": "AccountPurge",
          "debtor_id": 4294967296,
          "creditor_id": 2,
          "creation_date": "2098-12-31",
          "ts": "2099-12-31T00:00:00+00:00"
        }
        """,
            props,
        )

    props = MessageProperties(
        content_type="application/json", type="AccountPurge"
    )
    assert (
        consumer.process_message(
            b"""
    {
      "type": "AccountPurge",
      "debtor_id": 4294967296,
      "creditor_id": 3,
      "creation_date": "2098-12-31",
      "ts": "2099-12-31T00:00:00+00:00"
    }
    """,
            props,
        )
        is True
    )
