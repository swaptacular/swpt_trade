import pytest
import sqlalchemy
import flask_migrate
from datetime import datetime, timezone
from swpt_trade import create_app
from swpt_trade.extensions import db

config_dict = {
    "TESTING": True,
    "MIN_COLLECTOR_ID": 4294967296,
    "MAX_COLLECTOR_ID": 8589934591,
    "TURN_PHASE1_DURATION": "0",
    "TURN_PHASE2_DURATION": "0",
    "APP_ENABLE_CORS": True,
    "APP_DEBTOR_INFO_FETCH_BURST_COUNT": 1,
    "APP_SUPERUSER_SUBJECT_REGEX": "^creditors-superuser$",
    "APP_SUPERVISOR_SUBJECT_REGEX": "^creditors-supervisor$",
    "APP_CREDITOR_SUBJECT_REGEX": "^creditors:([0-9]+)$",
}


@pytest.fixture(scope="module")
def app():
    """Get a Flask application object."""

    app = create_app(config_dict)
    with app.app_context():
        flask_migrate.upgrade()
        yield app


@pytest.fixture(scope="function")
def db_session(app):
    """Get a Flask-SQLAlchmey session, with an automatic cleanup."""

    yield db.session

    # Cleanup:
    db.session.remove()
    for cmd in [
        "TRUNCATE TABLE configure_account_signal",
        "TRUNCATE TABLE prepare_transfer_signal",
        "TRUNCATE TABLE finalize_transfer_signal",
        "TRUNCATE TABLE fetch_debtor_info_signal",
        "TRUNCATE TABLE store_document_signal",
        "TRUNCATE TABLE discover_debtor_signal",
        "TRUNCATE TABLE confirm_debtor_signal",
        "TRUNCATE TABLE activate_collector_signal",
        "TRUNCATE TABLE debtor_info_document",
        "TRUNCATE TABLE debtor_locator_claim",
        "TRUNCATE TABLE debtor_info_fetch",
        "TRUNCATE TABLE trading_policy",
        "TRUNCATE TABLE worker_account",
        "TRUNCATE TABLE needed_worker_account",
        "TRUNCATE TABLE worker_turn",
    ]:
        db.session.execute(sqlalchemy.text(cmd))

    for cmd in [
        "TRUNCATE TABLE collector_account",
        "TRUNCATE TABLE turn",
        "TRUNCATE TABLE debtor_info",
        "TRUNCATE TABLE confirmed_debtor",
        "TRUNCATE TABLE currency_info",
        "TRUNCATE TABLE buy_offer",
        "TRUNCATE TABLE sell_offer",
        "TRUNCATE TABLE creditor_taking",
        "TRUNCATE TABLE collector_collecting",
        "TRUNCATE TABLE collector_sending",
        "TRUNCATE TABLE collector_receiving",
        "TRUNCATE TABLE collector_dispatching",
        "TRUNCATE TABLE creditor_giving",
    ]:
        db.session.execute(
            sqlalchemy.text(cmd),
            bind_arguments={"bind": db.engines["solver"]},
        )

    db.session.commit()


@pytest.fixture(scope="function")
def current_ts():
    return datetime.now(tz=timezone.utc)


@pytest.fixture()
def restore_sharding_realm(app):
    orig_sharding_realm = app.config["SHARDING_REALM"]
    orig_delete_parent_recs = app.config["DELETE_PARENT_SHARD_RECORDS"]
    yield
    app.config["DELETE_PARENT_SHARD_RECORDS"] = orig_delete_parent_recs
    app.config["SHARDING_REALM"] = orig_sharding_realm
