from __future__ import annotations
from sqlalchemy.sql.expression import null, or_, and_
from swpt_trade.extensions import db


class CollectorAccount(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String, nullable=False)
    status = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment=(
            "Collector account status: 0 means active; otherwise inactive."
        ),
    )


class Turn(db.Model):
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
    collection_deadline = db.Column(db.TIMESTAMP(timezone=True))
    collection_started_at = db.Column(db.TIMESTAMP(timezone=True))
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
    )


class DebtorInfo(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_info_iri = db.Column(db.String, primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    debtor_info_content_type = db.Column(db.String, nullable=False)
    debtor_info_sha256 = db.Column(db.LargeBinary, nullable=False)
    peg_debtor_info_iri = db.Column(db.String)
    peg_debtor_id = db.Column(db.BigInteger)
    peg_exchange_rate = db.Column(db.FLOAT)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["turn_id"], ["turn.turn_id"], ondelete="CASCADE"
        ),
    )


class ConfirmedDebtor(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)

    # NOTE: The rest of the columns are not be part of the primary
    # key, but should be included in the primary key index to allow
    # index-only scans. Because SQLAlchemy does not support this yet
    # (2024-01-19), the migration file should be edited so as not to
    # create a "normal" index, but create a "covering" index instead.
    debtor_info_iri = db.Column(db.String, nullable=False)
    debtor_info_content_type = db.Column(db.String, nullable=False)
    debtor_info_sha256 = db.Column(db.LargeBinary, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["turn_id"], ["turn.turn_id"], ondelete="CASCADE"
        ),
    )


class CurrencyInfo(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_info_iri = db.Column(db.String, primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    peg_debtor_info_iri = db.Column(db.String)
    peg_debtor_id = db.Column(db.BigInteger)
    peg_exchange_rate = db.Column(db.FLOAT)
    is_confirmed = db.Column(db.BOOLEAN, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["turn_id"], ["turn.turn_id"], ondelete="CASCADE"
        ),
        db.Index(
            "idx_confirmed_debtor_id",
            turn_id,
            debtor_id,
            unique=True,
            postgresql_where=is_confirmed,
        ),
    )


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
            ["debtor_id", "collector_id"],
            ["collector_account.debtor_id", "collector_account.creditor_id"],
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


class CreditorTaking(db.Model):
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
            ["debtor_id", "collector_id"],
            ["collector_account.debtor_id", "collector_account.creditor_id"],
            ondelete="RESTRICT",
        ),
        db.CheckConstraint(amount > 0),
    )


class CollectorCollecting(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    collector_hash = db.Column(db.SmallInteger, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["turn_id"], ["turn.turn_id"], ondelete="CASCADE"
        ),
        db.ForeignKeyConstraint(
            ["debtor_id", "collector_id"],
            ["collector_account.debtor_id", "collector_account.creditor_id"],
            ondelete="RESTRICT",
        ),
        db.CheckConstraint(amount > 0),
    )


class CollectorSending(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    from_collector_id = db.Column(db.BigInteger, primary_key=True)
    to_collector_id = db.Column(db.BigInteger, primary_key=True)
    from_collector_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["turn_id"], ["turn.turn_id"], ondelete="CASCADE"
        ),
        db.ForeignKeyConstraint(
            ["debtor_id", "from_collector_id"],
            ["collector_account.debtor_id", "collector_account.creditor_id"],
            ondelete="RESTRICT",
        ),
        db.ForeignKeyConstraint(
            ["debtor_id", "to_collector_id"],
            ["collector_account.debtor_id", "collector_account.creditor_id"],
            ondelete="RESTRICT",
        ),
        db.CheckConstraint(amount > 0),
    )


class CollectorReceiving(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    to_collector_id = db.Column(db.BigInteger, primary_key=True)
    from_collector_id = db.Column(db.BigInteger, primary_key=True)
    to_collector_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["turn_id"], ["turn.turn_id"], ondelete="CASCADE"
        ),
        db.ForeignKeyConstraint(
            ["debtor_id", "to_collector_id"],
            ["collector_account.debtor_id", "collector_account.creditor_id"],
            ondelete="RESTRICT",
        ),
        db.ForeignKeyConstraint(
            ["debtor_id", "from_collector_id"],
            ["collector_account.debtor_id", "collector_account.creditor_id"],
            ondelete="RESTRICT",
        ),
        db.CheckConstraint(amount > 0),
    )


class CreditorGiving(db.Model):
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
            ["debtor_id", "collector_id"],
            ["collector_account.debtor_id", "collector_account.creditor_id"],
            ondelete="RESTRICT",
        ),
        db.CheckConstraint(amount > 0),
    )
