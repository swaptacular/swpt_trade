from __future__ import annotations
from sqlalchemy.sql.expression import func, null, or_, and_
from swpt_trade.extensions import db
from .common import get_now_utc


class NeededWorkerAccount(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    configured_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
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
    last_config_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    last_config_seqnum = db.Column(db.Integer, nullable=False)
    negligible_amount = db.Column(db.REAL, nullable=False)
    config_flags = db.Column(db.Integer, nullable=False)
    config_data = db.Column(db.String, nullable=False)
    account_id = db.Column(db.String, nullable=False)
    debtor_info_iri = db.Column(db.String)
    debtor_info_content_type = db.Column(db.String)
    debtor_info_sha256 = db.Column(db.LargeBinary)
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
        db.CheckConstraint(negligible_amount >= 0.0),
        db.CheckConstraint(last_transfer_number >= 0),
        db.CheckConstraint(
            and_(demurrage_rate >= -100.0, demurrage_rate <= 0.0)
        ),
        db.CheckConstraint(commit_period >= 0),
        db.CheckConstraint(
            or_(
                debtor_info_sha256 == null(),
                func.octet_length(debtor_info_sha256) == 32,
            )
        ),
    )
