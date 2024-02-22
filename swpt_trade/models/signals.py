from __future__ import annotations
from flask import current_app
from marshmallow import Schema, fields
from swpt_pythonlib.utils import (
    i64_to_hex_routing_key,
    calc_bin_routing_key,
    calc_iri_routing_key,
)
from swpt_trade.extensions import (
    db,
    CREDITORS_OUT_EXCHANGE,
    TO_TRADE_EXCHANGE,
)
from .common import Signal, CT_AGENT


class classproperty(object):
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


class ConfigureAccountSignal(Signal):
    exchange_name = CREDITORS_OUT_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("ConfigureAccount")
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        ts = fields.DateTime()
        seqnum = fields.Integer()
        negligible_amount = fields.Float()
        config_data = fields.String()
        config_flags = fields.Integer()

    __marshmallow_schema__ = __marshmallow__()

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    ts = db.Column(db.TIMESTAMP(timezone=True), primary_key=True)
    seqnum = db.Column(db.Integer, primary_key=True)
    negligible_amount = db.Column(db.REAL, nullable=False)
    config_data = db.Column(db.String, nullable=False, default="")
    config_flags = db.Column(db.Integer, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.debtor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_CONFIGURE_ACCOUNTS_BURST_COUNT"]


class PrepareTransferSignal(Signal):
    exchange_name = CREDITORS_OUT_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("PrepareTransfer")
        creditor_id = fields.Integer()
        debtor_id = fields.Integer()
        coordinator_type = fields.Constant(CT_AGENT)
        coordinator_id = fields.Integer(
            attribute="creditor_id", dump_only=True
        )
        coordinator_request_id = fields.Integer()
        min_locked_amount = fields.Integer(
            attribute="locked_amount", dump_only=True
        )
        max_locked_amount = fields.Integer(
            attribute="locked_amount", dump_only=True
        )
        recipient = fields.String()
        min_interest_rate = fields.Float()
        max_commit_delay = fields.Integer()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_request_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    recipient = db.Column(db.String, nullable=False)
    locked_amount = db.Column(db.BigInteger, nullable=False)
    min_interest_rate = db.Column(db.Float, nullable=False)
    max_commit_delay = db.Column(db.Integer, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.debtor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_PREPARE_TRANSFERS_BURST_COUNT"]


class FinalizeTransferSignal(Signal):
    exchange_name = CREDITORS_OUT_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("FinalizeTransfer")
        creditor_id = fields.Integer()
        debtor_id = fields.Integer()
        transfer_id = fields.Integer()
        coordinator_type = fields.Constant(CT_AGENT)
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        committed_amount = fields.Integer()
        transfer_note_format = fields.String()
        transfer_note = fields.String()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    transfer_id = db.Column(db.BigInteger, nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    transfer_note_format = db.Column(db.String, nullable=False)
    transfer_note = db.Column(db.String, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.debtor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_FINALIZE_TRANSFERS_BURST_COUNT"]


class FetchDebtorInfoSignal(Signal):
    """Requests a debtor info document to be fetched from Internet.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("FetchDebtorInfo")
        iri = fields.String()
        debtor_id = fields.Integer()
        is_locator_fetch = fields.Boolean()
        is_discovery_fetch = fields.Boolean()
        recursion_level = fields.Integer()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    iri = db.Column(db.String, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    is_locator_fetch = db.Column(db.BOOLEAN, nullable=False)
    is_discovery_fetch = db.Column(db.BOOLEAN, nullable=False)
    recursion_level = db.Column(db.SmallInteger, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_iri_routing_key(self.iri)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_FETCH_DEBTOR_INFO_BURST_COUNT"]


class StoreDocumentSignal(Signal):
    """Requests a debtor info document to be stored.

    NOTE: Instead of sending a `StoreDocumentSignal`, a simpler
    approach would be to directly "upsert" the document in the
    database. The problem with this is that it is prone to database
    serialization errors, because documents are usually fetched and
    stored in large batches.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("StoreDocument")
        debtor_info_locator = fields.String()
        debtor_id = fields.Integer()
        peg_debtor_info_locator = fields.String()
        peg_debtor_id = fields.Integer()
        peg_exchange_rate = fields.Float()
        will_not_change_until = fields.DateTime()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    debtor_info_locator = db.Column(db.String, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    peg_debtor_info_locator = db.Column(db.String)
    peg_debtor_id = db.Column(db.BigInteger)
    peg_exchange_rate = db.Column(db.FLOAT)
    will_not_change_until = db.Column(db.TIMESTAMP(timezone=True))

    @property
    def routing_key(self):  # pragma: no cover
        return calc_iri_routing_key(self.debtor_info_locator)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_STORE_DOCUMENT_BURST_COUNT"]


class DiscoverDebtorSignal(Signal):
    """Starts the debtor-confirmation process for a given debtor.

    The `iri` field specifies an Internationalized Resource Identifier
    (IRI), from which a debtor info document for the given debtor can
    be fetched. Note that normally the given IRI will not be the same
    as the debtor's "debtor info locator".

    NOTE: A `DiscoverDebtorSignal` should be sent periodically for
    every collector account which is "alive". The easiest way to
    achieve this is to arrange received SMP `UpdateAccount` messages
    (aka account heartbeats) to periodically trigger the sending of
    `DiscoverDebtorSignal`s.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("DiscoverDebtor")
        debtor_id = fields.Integer()
        iri = fields.String()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    iri = db.Column(db.String, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.debtor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_DISCOVER_DEBTOR_BURST_COUNT"]


class ConfirmDebtorSignal(Signal):
    """Finalizes the debtor-confirmation process for a given debtor.

    This signal informs that the given debtor successfully claimed the
    given debtor info locator.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("ConfirmDebtor")
        debtor_id = fields.Integer()
        debtor_info_locator = fields.String()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    debtor_info_locator = db.Column(db.String, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.debtor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_CONFIRM_DEBTOR_BURST_COUNT"]
