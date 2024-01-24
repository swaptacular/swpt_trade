import pytest
import sqlalchemy
import flask_migrate
from datetime import datetime, timezone
from swpt_trade import create_app
from swpt_trade.extensions import db

config_dict = {
    "TESTING": True,
    "MIN_CREDITOR_ID": 4294967296,
    "MAX_CREDITOR_ID": 8589934591,
    "APP_ENABLE_CORS": True,
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
    ]:
        db.session.execute(sqlalchemy.text(cmd))

    for cmd in [
        "TRUNCATE TABLE collector_account",
        "TRUNCATE TABLE turn",
        "TRUNCATE TABLE confirmed_debtor",
        "TRUNCATE TABLE currency_info",
        "TRUNCATE TABLE sell_offer",
        "TRUNCATE TABLE creditor_taking",
        "TRUNCATE TABLE collector_collecting",
        "TRUNCATE TABLE collector_sending",
        "TRUNCATE TABLE collector_receiving",
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
