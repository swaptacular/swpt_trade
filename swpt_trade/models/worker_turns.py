from __future__ import annotations
from datetime import date
from .common import get_now_utc
from sqlalchemy.sql.expression import null, or_, and_
from sqlalchemy.orm import foreign
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
    amount = db.Column(
        db.BigInteger,
        comment=(
            "Can be negative or zero (the trader wants to sell), or positive"
            " (the trader wants to buy). When selling, and the `transfer_id`"
            " column is being set to a non-NULL value, the amount will be"
            " re-calculated to be equal to the locked amount reduced in"
            " accordance with the effective demurrage rate. Also, when"
            " selling, and the `finalized_at` column is being set to a"
            " non-NULL value, the amount will be re-set to be equal to the"
            " committed amount with a negative sign."
        ),
        nullable=False,
    )
    transfer_id = db.Column(db.BigInteger)
    finalized_at = db.Column(
        db.TIMESTAMP(timezone=True),
        comment="The timestamp of the sent `FinalizeTransfer` SMP message.",
    )
    released_at = db.Column(db.TIMESTAMP(timezone=True))
    account_creation_date = db.Column(db.DATE)
    account_last_transfer_number = db.Column(db.BigInteger)
    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        db.CheckConstraint(or_(finalized_at == null(), transfer_id != null())),
        db.CheckConstraint(
            or_(
                released_at == null(),
                and_(
                    finalized_at != null(),
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


class CreditorParticipation(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment=(
            "Can be positive or negative, but can not be zero. A positive"
            " number indicates that this amount should be given to the"
            " creditor. A negative number indicates that this amount should"
            " be taken from the creditor."
        ),
    )
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount != 0),
        {
            "comment": (
                'Indicates that the given amount must be given or taken'
                ' to/from the given creditor as part of the given trading'
                ' turn. During the phase 3 of each turn, "worker" servers'
                ' will move the records from the "creditor_giving" and'
                ' and "creditor_taking" solver tables to this table.'
            ),
        },
    )


class WorkerCollecting(db.Model):
    collector_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    collected_amount = db.Column(db.BigInteger)
    __table_args__ = (
        db.CheckConstraint(collected_amount > 0),
        db.Index(
            "idx_worker_collecting_not_done",
            collector_id,
            turn_id,
            debtor_id,
            postgresql_where=collected_amount == null(),
        ),
        {
            "comment": (
                'Indicates that the given amount will be withdrawn (collected)'
                " from the given creditor's account, as part of the given"
                ' trading turn, and will be transferred to the given'
                ' collector. During the phase 3 of each turn, "worker" servers'
                ' will move the records from the "collector_collecting" solver'
                ' table to this table.'
            ),
        },
        # NOTE: Normally, there should be a foreign key constraint
        # connecting each row in this table to a row in the
        # "sending_trigger. For performance reasons, however, this
        # foreign key is not declared.
    )


class SendingTrigger(db.Model):
    collector_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    expected_collected_amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment=(
            'The sum of all amounts for the corresponding records in the'
            ' "worker_collecting" table.'
        ),
    )
    total_collected_amount = db.Column(
        db.BigInteger,
        comment=(
            "A non-NULL value indicates that all transfers for the"
            ' corresponding records in the "worker_collecting" table have'
            ' been received.'
        ),
    )
    __table_args__ = (
        db.CheckConstraint(expected_collected_amount > 0),
        db.CheckConstraint(total_collected_amount >= 0),
        db.Index(
            "idx_sending_trigger_all_collected",
            collector_id,
            turn_id,
            debtor_id,
            postgresql_where=(
                total_collected_amount == expected_collected_amount
            ),
        ),
        {
            "comment": (
                'Indicates that once all transfers for the corresponding'
                ' records in the "worker_collecting" table have been received,'
                ' the given collector should start sending funds to other'
                ' collectors, as stated in the "worker_sending" table. Note'
                ' that even if there are no relevant records in the'
                ' "worker_sending" table, when there is at least one'
                ' corresponding record in the "worker_collecting" table,'
                ' there will be a record in the "sending_trigger" table as'
                ' well.'
            ),
        },
    )

    @property
    def should_start_sending(self) -> bool:
        return self.total_collected_amount == self.expected_collected_amount


class WorkerSending(db.Model):
    from_collector_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    to_collector_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        {
            "comment": (
                'Indicates that the given amount must be transferred (sent)'
                ' to another collector account, as part of the given trading'
                ' turn. During the phase 3 of each turn, "worker" servers will'
                ' move the records from the "collector_sending" solver table'
                ' to this table.'
            ),
        },
    )


class WorkerReceiving(db.Model):
    to_collector_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    from_collector_id = db.Column(db.BigInteger, primary_key=True)
    received_amount = db.Column(db.BigInteger)
    __table_args__ = (
        db.CheckConstraint(received_amount > 0),
        db.Index(
            "idx_worker_receiving_not_done",
            to_collector_id,
            turn_id,
            debtor_id,
            postgresql_where=received_amount == null(),
        ),
        {
            "comment": (
                'Indicates the given amount will be transferred (received)'
                ' from another collector account, as part of the given trading'
                ' turn. During the phase 3 of each turn, "worker" servers will'
                ' move the records from the "collector_receiving" solver table'
                ' to this table.'
            ),
        },
        # NOTE: Normally, there should be a foreign key constraint
        # connecting each row in this table to a row in the
        # "dispatching_trigger. For performance reasons, however, this
        # foreign key is not declared.
    )


class DispatchingTrigger(db.Model):
    collector_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    expected_collected_amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment=(
            'The sum of all amounts for the corresponding records in the'
            ' "worker_collecting" table.'
        ),
    )
    expected_sent_amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment=(
            'The sum of all amounts for the corresponding records in the'
            ' "worker_sending" table.'
        ),
    )
    total_received_amount = db.Column(
        db.BigInteger,
        comment=(
            "A non-NULL value indicates that all transfers for the"
            ' corresponding records in the "worker_receiving" table have'
            ' been received.'
        ),
    )
    __table_args__ = (
        db.CheckConstraint(expected_collected_amount >= 0),
        db.CheckConstraint(expected_sent_amount >= 0),
        db.CheckConstraint(expected_sent_amount <= expected_collected_amount),
        db.CheckConstraint(total_received_amount >= 0),
        db.Index(
            "idx_dispatching_trigger_all_received",
            collector_id,
            turn_id,
            debtor_id,
            postgresql_where=total_received_amount != null(),
        ),
        {
            "comment": (
                'Indicates that once all transfers for the corresponding'
                ' records in the "worker_collecting" and "worker_receiving"'
                ' tables have been received, the given collector should start'
                " dispatching funds to creditors' accounts, as stated in the"
                ' "worker_dispatching" table.'
            ),
        },
    )

    sending_trigger = db.relationship(
        SendingTrigger,
        primaryjoin=and_(
            SendingTrigger.collector_id == foreign(collector_id),
            SendingTrigger.turn_id == foreign(turn_id),
            SendingTrigger.debtor_id == foreign(debtor_id),
        ),
    )

    @property
    def should_start_dispatching(self) -> bool:
        return (
            self.total_received_amount is not None
            and (
                (st := self.sending_trigger) is None
                or st.total_collected_amount == st.expected_collected_amount
            )
        )

    @property
    def available_amount(self) -> int:
        return (
            + self.expected_collected_amount
            - self.expected_sent_amount
            + (self.total_received_amount or 0)
        )


class WorkerDispatching(db.Model):
    collector_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        {
            "comment": (
                'Indicates that the given amount must be deposited'
                ' (dispatched) to the given customer account, as part of the'
                ' given trading turn. During the phase 3 of each turn,'
                ' "worker" servers will move the records from the'
                ' "collector_dispatching" solver table to this table.'
            ),
        },
    )
