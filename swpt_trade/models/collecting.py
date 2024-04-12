from __future__ import annotations
from sqlalchemy.sql.expression import and_
from swpt_trade.extensions import db
from .common import get_now_utc


class NeededWorkerAccount(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    configured_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    __table_args__ = (
        {
            "comment": (
                'Represents the fact that a "worker" server has requested'
                ' the configuration (aka creation) of a Swaptacular account,'
                ' which will be used to collect and dispatch transfers.'
            ),
        },
    )


class WorkerAccount(db.Model):
    CONFIG_SCHEDULED_FOR_DELETION_FLAG = 1 << 0

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, nullable=False)
    last_change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    last_change_seqnum = db.Column(db.Integer, nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    interest = db.Column(db.FLOAT, nullable=False)
    interest_rate = db.Column(db.REAL, nullable=False)
    last_interest_rate_change_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False
    )
    config_flags = db.Column(db.Integer, nullable=False)
    account_id = db.Column(db.String, nullable=False)
    debtor_info_iri = db.Column(db.String)
    last_transfer_number = db.Column(db.BigInteger, nullable=False)
    last_transfer_committed_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False
    )
    demurrage_rate = db.Column(db.FLOAT, nullable=False)
    commit_period = db.Column(db.Integer, nullable=False)
    transfer_note_max_bytes = db.Column(db.Integer, nullable=False)
    last_heartbeat_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    __table_args__ = (
        db.CheckConstraint(interest_rate >= -100.0),
        db.CheckConstraint(transfer_note_max_bytes >= 0),
        db.CheckConstraint(last_transfer_number >= 0),
        db.CheckConstraint(
            and_(demurrage_rate >= -100.0, demurrage_rate <= 0.0)
        ),
        db.CheckConstraint(commit_period >= 0),
        {
            "comment": (
                'Represents an existing Swaptacular account, managed by a '
                ' "worker" server. The account is used to collect and dispatch'
                ' transfers.'
            ),
        },
    )


class RecentlyNeededCollector(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    needed_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    __table_args__ = (
        db.CheckConstraint(debtor_id != 0),
        {
            "comment": (
                'Indicates that the creation of a collector account for the'
                ' currency with the given debtor ID has been recently'
                ' requested. This information is used to prevent "worker"'
                ' servers from making repetitive queries to the central'
                " database."
            ),
        },
    )


class ActiveCollector(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    collector_id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String, nullable=False)
    __table_args__ = (
        db.CheckConstraint(account_id != ""),
        {
            "comment": (
                'Represents an active Swaptacular account which can be'
                ' used to collect and dispatch transfers. Each "Worker"'
                ' servers will maintain its own copy of this table (that is:'
                ' no rows-sharding) by periodically copying the relevant'
                ' records from the solver\'s "collector_account" table.'
                ' "Worker" servers will use this local copy so as to avoid'
                ' querying the central database too often.'
            ),
        },
    )
