from __future__ import annotations
from sqlalchemy.sql.expression import null, or_, and_
from swpt_trade.extensions import db


class CollectorAccount(db.Model):
    __bind_key__ = "solver"
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    collector_id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String, nullable=False)


class Turn(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    started_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    phase = db.Column(
        db.SmallInteger,
        nullable=False,
        comment=(
            "Turn's phase: 1) gathering currencies info; 2) gathering"
            " buy and sell offers; 3) giving and taking; 4) done."
        ),
    )
    phase_deadline = db.Column(db.TIMESTAMP(timezone=True))
    collection_started_at = db.Column(db.TIMESTAMP(timezone=True))
    collection_deadline = db.Column(db.TIMESTAMP(timezone=True))
    __table_args__ = (
        db.CheckConstraint(
            and_(
                or_(phase < 2, collection_deadline != null()),
                or_(phase < 3, collection_started_at != null()),
            )
        ),
        db.Index(
            "idx_phase",
            phase,
            postgresql_where=phase < 4,
        ),
        db.Index("idx_started_at", started_at),
    )


class DebtorInfo(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_info_locator = db.Column(db.String, primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    peg_debtor_info_locator = db.Column(db.String)
    peg_debtor_id = db.Column(db.BigInteger)
    peg_exchange_rate = db.Column(db.FLOAT)


class ConfirmedDebtor(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)

    # NOTE: The `debtor_info_locator` column is not be part of the
    # primary key, but should be included in the primary key index to
    # allow index-only scans. Because SQLAlchemy does not support this
    # yet (2024-01-19), the migration file should be edited so as not
    # to create a "normal" index, but create a "covering" index
    # instead.
    debtor_info_locator = db.Column(db.String, nullable=False)


class CurrencyInfo(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_info_locator = db.Column(db.String, primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    peg_debtor_info_locator = db.Column(db.String)
    peg_debtor_id = db.Column(db.BigInteger)
    peg_exchange_rate = db.Column(db.FLOAT)
    is_confirmed = db.Column(db.BOOLEAN, nullable=False)
    __table_args__ = (
        db.Index(
            "idx_confirmed_debtor_id",
            turn_id,
            debtor_id,
            unique=True,
            postgresql_where=is_confirmed,
        ),
    )


class SellOffer(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )


class BuyOffer(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )


class CreditorTaking(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )


class CollectorCollecting(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    collector_hash = db.Column(db.SmallInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )


class CollectorSending(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    from_collector_id = db.Column(db.BigInteger, primary_key=True)
    to_collector_id = db.Column(db.BigInteger, primary_key=True)
    from_collector_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )


class CollectorReceiving(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    to_collector_id = db.Column(db.BigInteger, primary_key=True)
    from_collector_id = db.Column(db.BigInteger, primary_key=True)
    to_collector_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )


class CreditorGiving(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )
