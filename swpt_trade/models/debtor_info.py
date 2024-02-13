from __future__ import annotations
from sqlalchemy.sql.expression import null, or_, and_
from swpt_trade.extensions import db
from .common import get_now_utc


class DebtorInfoDocument(db.Model):
    debtor_info_locator = db.Column(db.String, primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    peg_debtor_info_locator = db.Column(db.String)
    peg_debtor_id = db.Column(db.BigInteger)
    peg_exchange_rate = db.Column(db.FLOAT)
    will_not_change_until = db.Column(db.TIMESTAMP(timezone=True))
    fetched_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    __table_args__ = (
        db.CheckConstraint(
            or_(
                peg_debtor_info_locator == null(),
                and_(
                    peg_debtor_id != null(),
                    peg_exchange_rate != null(),
                    peg_exchange_rate >= 0.0,
                ),
            )
        ),
    )


class AnchorDebtor(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    debtor_info_locator = db.Column(db.String)  # NOTE: nulls are allowed!
    locator_claimed_at = db.Column(db.TIMESTAMP(timezone=True))
    latest_debtor_info_fetch_at = db.Column(db.TIMESTAMP(timezone=True))
    __table_args__ = (
        db.CheckConstraint(
            or_(
                debtor_info_locator == null(),
                and_(
                    locator_claimed_at != null(),
                    latest_debtor_info_fetch_at != null(),
                ),
            )
        ),
        db.Index(
            "idx_anchor_debtor_fetch_at", latest_debtor_info_fetch_at
        ),
    )


class DebtorInfoFetch(db.Model):
    iri = db.Column(db.String, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    is_locator = db.Column(db.BOOLEAN, nullable=False, default=False)
    is_confirmation = db.Column(db.BOOLEAN, nullable=False, default=False)
    distance_to_anchor = db.Column(db.SmallInteger, nullable=False, default=0)
    inserted_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    attempts_count = db.Column(db.SmallInteger, nullable=False, default=0)
    latest_attempt_at = db.Column(db.TIMESTAMP(timezone=True))
    latest_attempt_errorcode = db.Column(db.SmallInteger)
    next_attempt_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    __table_args__ = (
        db.CheckConstraint(distance_to_anchor >= 0),
        db.CheckConstraint(attempts_count >= 0),
        db.CheckConstraint(
            or_(attempts_count == 0, latest_attempt_at != null())
        ),
        db.Index("idx_debtor_info_fetch_next_attempt_at", next_attempt_at),
    )
