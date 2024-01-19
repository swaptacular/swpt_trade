from __future__ import annotations
from sqlalchemy.sql.expression import null, or_, and_
from swpt_trade.extensions import db


class Turn(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    phase = db.Column(db.SmallInteger, nullable=False)
    phase_deadline = db.Column(db.TIMESTAMP(timezone=True), nullable=False)


class CurrencyInfo(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_uri = db.Column(db.String, primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    peg_debtor_uri = db.Column(db.String)
    peg_debtor_id = db.Column(db.BigInteger)
    peg_exchange_rate = db.Column(db.FLOAT)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["turn_id"], ["turn.turn_id"], ondelete="CASCADE"
        ),
        db.CheckConstraint(
            or_(
                and_(
                    peg_debtor_uri == null(),
                    peg_debtor_id == null(),
                    peg_exchange_rate == null(),
                ),
                and_(
                    peg_debtor_uri != null(),
                    peg_debtor_id != null(),
                    peg_exchange_rate != null(),
                ),
            )
        ),
    )


class ConfirmedCurrency(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)

    # NOTE: The `debtor_uri` column is not be part of the primary key,
    # but should be included in the primary key index to allow
    # index-only scans. Because SQLAlchemy does not support this yet
    # (2024-01-19), the migration file should be edited so as not to
    # create a "normal" index, but create a "covering" index instead.
    debtor_uri = db.Column(db.String, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["turn_id"], ["turn.turn_id"], ondelete="CASCADE"
        ),
    )


class Collector(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_hash = db.Column(db.SmallInteger, nullable=False)
    account_id = db.Column(db.String, nullable=False)


class SellOffer(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["turn_id"], ["turn.turn_id"], ondelete="CASCADE"
        ),
        db.ForeignKeyConstraint(
            ["collector_id", "debtor_id"],
            ["collector.creditor_id", "collector.debtor_id"],
            ondelete="RESTRICT",
        ),
        db.CheckConstraint(amount > 0),
    )


class BuyOffer(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["turn_id"], ["turn.turn_id"], ondelete="CASCADE"
        ),
        db.CheckConstraint(amount > 0),
    )


class TraderTaking(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["turn_id"], ["turn.turn_id"], ondelete="CASCADE"
        ),
        db.ForeignKeyConstraint(
            ["collector_id", "debtor_id"],
            ["collector.creditor_id", "collector.debtor_id"],
            ondelete="RESTRICT",
        ),
        db.CheckConstraint(amount > 0),
    )


class CollectorGiving(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    from_creditor_id = db.Column(db.BigInteger, primary_key=True)
    to_creditor_id = db.Column(db.BigInteger, primary_key=True)
    from_creditor_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["turn_id"], ["turn.turn_id"], ondelete="CASCADE"
        ),
        db.ForeignKeyConstraint(
            ["from_creditor_id", "debtor_id"],
            ["collector.creditor_id", "collector.debtor_id"],
            ondelete="RESTRICT",
        ),
        db.ForeignKeyConstraint(
            ["to_creditor_id", "debtor_id"],
            ["collector.creditor_id", "collector.debtor_id"],
            ondelete="RESTRICT",
        ),
        db.CheckConstraint(amount > 0),
    )


class CollectorTaking(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    to_creditor_id = db.Column(db.BigInteger, primary_key=True)
    from_creditor_id = db.Column(db.BigInteger, primary_key=True)
    to_creditor_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["turn_id"], ["turn.turn_id"], ondelete="CASCADE"
        ),
        db.ForeignKeyConstraint(
            ["to_creditor_id", "debtor_id"],
            ["collector.creditor_id", "collector.debtor_id"],
            ondelete="RESTRICT",
        ),
        db.ForeignKeyConstraint(
            ["from_creditor_id", "debtor_id"],
            ["collector.creditor_id", "collector.debtor_id"],
            ondelete="RESTRICT",
        ),
        db.CheckConstraint(amount > 0),
    )


class TraderGiving(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["turn_id"], ["turn.turn_id"], ondelete="CASCADE"
        ),
        db.ForeignKeyConstraint(
            ["collector_id", "debtor_id"],
            ["collector.creditor_id", "collector.debtor_id"],
            ondelete="RESTRICT",
        ),
        db.CheckConstraint(amount > 0),
    )
