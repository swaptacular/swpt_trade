import pytest
from datetime import timedelta
from swpt_trade.extensions import db
from swpt_trade import models as m
from swpt_pythonlib.utils import ShardingRealm
from swpt_trade.fetch_debtor_infos import (
    FetchResult,
    perform_debtor_info_fetches,
)


@pytest.fixture()
def restore_expiry(app):
    orig_debtor_info_expiry_days = app.config["APP_DEBTOR_INFO_EXPIRY_DAYS"]
    yield
    app.config["APP_DEBTOR_INFO_EXPIRY_DAYS"] = orig_debtor_info_expiry_days


def test_last_fetch_retry(mocker, app, db_session, restore_expiry):
    app.config["APP_DEBTOR_INFO_EXPIRY_DAYS"] = -10.0

    def perform_fetches(fetches, **kwargs):
        return [
            FetchResult(
                fetch=f,
                errorcode=500,
                retry=True,
            ) for f in fetches
        ]

    mocker.patch(
        "swpt_trade.fetch_debtor_infos._perform_fetches",
        new=perform_fetches,
    )

    db.session.add(
        m.DebtorInfoFetch(
            iri="https://example.com/666",
            debtor_id=666,
            is_locator_fetch=True,
            is_discovery_fetch=False,
            ignore_cache=True,
        )
    )
    db.session.commit()

    assert perform_debtor_info_fetches(1, 0.1) == 1
    assert len(m.DebtorInfoFetch.query.all()) == 0


def test_cached_and_wrong_shard(
        mocker,
        app,
        db_session,
        current_ts,
        restore_sharding_realm,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")

    def perform_fetches(fetches, **kwargs):
        assert len(fetches) == 0
        return []

    mocker.patch(
        "swpt_trade.fetch_debtor_infos._perform_fetches",
        new=perform_fetches,
    )

    db.session.add(
        m.DebtorInfoDocument(
            debtor_info_locator="https://example.com/666",
            debtor_id=666,
            will_not_change_until=current_ts + timedelta(days=1000),
        )
    )
    db.session.add(
        m.DebtorInfoFetch(
            iri="https://example.com/666",
            debtor_id=666,
            is_locator_fetch=True,
            is_discovery_fetch=False,
            ignore_cache=False,
        )
    )
    db.session.add(
        m.DebtorInfoFetch(
            iri="https://example.com/888",
            debtor_id=888,
            is_locator_fetch=True,
            is_discovery_fetch=False,
            ignore_cache=True,
        )
    )
    db.session.commit()

    assert perform_debtor_info_fetches(1, 0.1) == 2
    assert len(m.DebtorInfoFetch.query.all()) == 0
    assert len(m.DebtorInfoDocument.query.all()) == 1


def test_perform_debtor_info_fetches(mocker, app, db_session, current_ts):
    def perform_fetches(fetches, **kwargs):
        return [
            (
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
                ) if f.iri.startswith("https://") else FetchResult(
                    fetch=f,
                    errorcode=500,
                    retry=True,
                )
            ) for f in fetches
        ]

    mocker.patch(
        "swpt_trade.fetch_debtor_infos._perform_fetches",
        new=perform_fetches,
    )

    dif1 = m.DebtorInfoFetch(
        iri="https://example.com/666",
        debtor_id=666,
        is_locator_fetch=True,
        is_discovery_fetch=True,
        ignore_cache=True,
    )
    dif2 = m.DebtorInfoFetch(
        iri="wrong IRI",
        debtor_id=999,
        is_locator_fetch=True,
        is_discovery_fetch=False,
        ignore_cache=False,
    )
    db.session.add(dif1)
    db.session.add(dif2)
    db.session.commit()

    assert perform_debtor_info_fetches(1, 0.1) == 2

    fetches = m.DebtorInfoFetch.query.all()
    assert len(fetches) == 1
    fetches[0].iri == "wrong IRI"
    fetches[0].debtor_id == 999
    fetches[0].is_locator_fetch is True
    fetches[0].is_discovery_fetch is False
    fetches[0].ignore_cache is False
    fetches[0].recursion_level == 0
    fetches[0].attempts_count == 1
    fetches[0].latest_attempt_at is not None
    fetches[0].latest_attempt_errorcode == 500
    fetches[0].next_attempt_at > current_ts + timedelta(seconds=15)

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
