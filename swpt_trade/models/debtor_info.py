from __future__ import annotations
from datetime import datetime, timedelta
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
        db.CheckConstraint(peg_exchange_rate >= 0.0),
        db.CheckConstraint(
            or_(
                and_(
                    peg_debtor_info_locator == null(),
                    peg_debtor_id == null(),
                    peg_exchange_rate == null(),
                ),
                and_(
                    peg_debtor_info_locator != null(),
                    peg_debtor_id != null(),
                    peg_exchange_rate != null(),
                ),
            )
        ),
    )

    def has_expired(
            self,
            current_ts: datetime,
            expiry_period: timedelta,
    ) -> bool:
        frozen_until = self.will_not_change_until
        is_frozen = frozen_until is not None and frozen_until > current_ts
        return not is_frozen and current_ts - self.fetched_at > expiry_period


class DebtorLocatorClaim(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    debtor_info_locator = db.Column(db.String)
    latest_locator_fetch_at = db.Column(db.TIMESTAMP(timezone=True))
    latest_discovery_fetch_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    __table_args__ = (
        db.CheckConstraint(
            or_(
                and_(
                    debtor_info_locator == null(),
                    latest_locator_fetch_at == null(),
                ),
                and_(
                    debtor_info_locator != null(),
                    latest_locator_fetch_at != null(),
                ),
            )
        ),
        db.Index(
            "idx_debtor_locator_claim_latest_locator_fetch_at",
            latest_locator_fetch_at,
            postgresql_where=latest_locator_fetch_at != null(),
        ),
    )


class DebtorInfoFetch(db.Model):
    iri = db.Column(db.String, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    is_locator_fetch = db.Column(db.BOOLEAN, nullable=False, default=False)
    is_discovery_fetch = db.Column(db.BOOLEAN, nullable=False, default=False)
    recursion_level = db.Column(db.SmallInteger, nullable=False, default=0)
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
        db.CheckConstraint(recursion_level >= 0),
        db.CheckConstraint(
            or_(
                and_(
                    attempts_count == 0,
                    latest_attempt_at == null(),
                    latest_attempt_errorcode == null(),
                ),
                and_(
                    attempts_count > 0,
                    latest_attempt_at != null(),
                ),
            )
        ),
        db.Index("idx_debtor_info_fetch_next_attempt_at", next_attempt_at),
    )
