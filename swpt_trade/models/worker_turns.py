from __future__ import annotations
from datetime import date
from .common import get_now_utc
from sqlalchemy.sql.expression import null, or_, and_
from swpt_trade.extensions import db


class WorkerTurn(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True, autoincrement=False)
    started_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    base_debtor_info_locator = db.Column(db.String, nullable=False)
    base_debtor_id = db.Column(db.BigInteger, nullable=False)
    max_distance_to_base = db.Column(db.SmallInteger, nullable=False)
    min_trade_amount = db.Column(db.BigInteger, nullable=False)
    phase = db.Column(db.SmallInteger, nullable=False)
    phase_deadline = db.Column(db.TIMESTAMP(timezone=True))
    collection_started_at = db.Column(db.TIMESTAMP(timezone=True))
    collection_deadline = db.Column(db.TIMESTAMP(timezone=True))
    worker_turn_subphase = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment=(
            "The worker may divide the processing of each phase to one or more"
            " sub-phases. The initial sub-phase is always `0`, and the final"
            " sub-phase is always `10`. Sequential sub-phases do not need to"
            " be (and normally will not be) represented by sequential"
            " numbers. This gives the freedom to add sub-phases if necessary."
        ),
    )
    __table_args__ = (
        db.CheckConstraint(base_debtor_id != 0),
        db.CheckConstraint(max_distance_to_base > 0),
        db.CheckConstraint(min_trade_amount > 0),
        db.CheckConstraint(and_(phase > 0, phase <= 3)),
        db.CheckConstraint(or_(phase > 2, phase_deadline != null())),
        db.CheckConstraint(or_(phase < 2, collection_deadline != null())),
        db.CheckConstraint(or_(phase < 3, collection_started_at != null())),
        db.CheckConstraint(
            and_(worker_turn_subphase >= 0, worker_turn_subphase <= 10)
        ),
        db.Index(
            "idx_worker_turn_phase",
            phase,
            postgresql_where=phase < 3,
        ),
        db.Index(
            "idx_worker_turn_subphase",
            worker_turn_subphase,
            postgresql_where=worker_turn_subphase < 10,
        ),
        {
            "comment": (
                'Represents a circular trading round in which a "worker"'
                ' server participates. "Worker" servers will watch for'
                " new and changed rows in the solver's `turn` table, and will"
                ' copy them off.'
            ),
        },
    )


# This sequence is used to generate `coordinator_request_id`s for the
# issued `PrepareTransfer` SMP messages.
#
# NOTE:
# `op.execute(CreateSequence(Sequence('coordinator_request_id_seq')))`
# should be manually added to the generated migration file.
cr_seq = db.Sequence(
    "coordinator_request_id_seq", metadata=db.Model.metadata
)


class AccountLock(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    initiated_at = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment="The timestamp of the sent `PrepareTransfer` SMP message.",
    )
    coordinator_request_id = db.Column(
        db.BigInteger, nullable=False, server_default=cr_seq.next_value()
    )
    transfer_id = db.Column(db.BigInteger)
    amount = db.Column(
        db.BigInteger,
        comment=(
            "Can be negative or zero (the trader wants to sell), or positive"
            " (the trader wants to buy). When selling, and the `transfer_id`"
            " column is being set to a non-NULL value, the amount will be"
            " re-calculated to be equal to the locked amount reduced in"
            " accordance with the effective demurrage rate."
        ),
        nullable=False,
    )
    committed_amount = db.Column(db.BigInteger, nullable=False, default=0)
    finalized_at = db.Column(db.TIMESTAMP(timezone=True))
    released_at = db.Column(db.TIMESTAMP(timezone=True))
    account_creation_date = db.Column(db.DATE)
    account_last_transfer_number = db.Column(db.BigInteger)
    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        db.CheckConstraint(committed_amount >= 0),
        db.CheckConstraint(or_(committed_amount == 0, finalized_at != null())),
        db.CheckConstraint(or_(finalized_at == null(), transfer_id != null())),
        db.CheckConstraint(or_(released_at == null(), finalized_at != null())),
        db.CheckConstraint(
            or_(
                released_at == null(),
                and_(
                    account_creation_date != null(),
                    account_last_transfer_number != null(),
                ),
            )
        ),
        db.ForeignKeyConstraint(["turn_id"], ["worker_turn.turn_id"]),
        db.Index("idx_lock_account_turn_id", turn_id),
        db.Index(
            "idx_lock_account_coordinator_request_id",
            coordinator_request_id,
            unique=True,
        ),
        {
            "comment": (
                "Represents an attempt to arrange the participation of a"
                " given account in a given trading turn. Normally, this"
                " includes sending a `PrepareTransfer` SMP message."
            ),
        },
    )

    worker_turn = db.relationship("WorkerTurn")

    @property
    def is_self_lock(self):
        return self.creditor_id == self.collector_id

    def is_in_force(self, acd: date, altn: int) -> bool:
        return not (
            self.released_at is not None
            and (
                self.account_creation_date < acd
                or (
                    self.account_creation_date == acd
                    and self.account_last_transfer_number <= altn
                )
            )
        )
