import logging
import os
import time
import signal
import sys
import click
import pika
from typing import Any
from datetime import datetime, timezone
from flask import current_app
from flask.cli import with_appcontext
from flask_sqlalchemy.model import Model
from swpt_pythonlib.multiproc_utils import (
    spawn_worker_processes,
    try_unblock_signals,
    HANDLED_SIGNALS,
)
from swpt_pythonlib.flask_signalbus import SignalBus, get_models_to_flush
from swpt_trade import procedures
from swpt_trade.roll_turns import try_to_advance_turn_to_phase3


@click.group("swpt_trade")
def swpt_trade():
    """Perform swpt_trade specific operations."""


@swpt_trade.command()
@with_appcontext
def subscribe():  # pragma: no cover
    """Declare a RabbitMQ queue, and subscribe it to receive incoming
    messages.

    The value of the PROTOCOL_BROKER_QUEUE_ROUTING_KEY configuration
    variable will be used as a binding key for the created queue. The
    default binding key is "#".

    This is mainly useful during development and testing.

    """

    from .extensions import (
        CREDITORS_IN_EXCHANGE,
        CREDITORS_OUT_EXCHANGE,
        TO_TRADE_EXCHANGE,
    )

    CA_TRADE_EXCHANGE = "ca.trade"
    logger = logging.getLogger(__name__)
    queue_name = current_app.config["PROTOCOL_BROKER_QUEUE"]
    routing_key = current_app.config["PROTOCOL_BROKER_QUEUE_ROUTING_KEY"]
    dead_letter_queue_name = queue_name + ".XQ"
    broker_url = current_app.config["PROTOCOL_BROKER_URL"]
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()

    # declare exchanges
    channel.exchange_declare(
        CREDITORS_IN_EXCHANGE, exchange_type="headers", durable=True
    )
    channel.exchange_declare(
        CREDITORS_OUT_EXCHANGE, exchange_type="topic", durable=True
    )
    channel.exchange_declare(
        TO_TRADE_EXCHANGE, exchange_type="topic", durable=True
    )
    channel.exchange_declare(
        CA_TRADE_EXCHANGE, exchange_type="x-random", durable=True
    )

    # declare exchange bindings
    channel.exchange_bind(
        source=TO_TRADE_EXCHANGE,
        destination=CA_TRADE_EXCHANGE,
    )
    logger.info(
        'Created a binding from "%s" to the "%s" exchange.',
        TO_TRADE_EXCHANGE,
        CA_TRADE_EXCHANGE,
    )
    channel.exchange_bind(
        source=CREDITORS_IN_EXCHANGE,
        destination=CA_TRADE_EXCHANGE,
        arguments={
            "x-match": "all",
            "ca-trade": True,
        },
    )
    logger.info(
        'Created a binding from "%s" to the "%s" exchange.',
        CREDITORS_IN_EXCHANGE,
        CA_TRADE_EXCHANGE,
    )

    # declare a corresponding dead-letter queue
    channel.queue_declare(dead_letter_queue_name, durable=True)
    logger.info('Declared "%s" dead-letter queue.', dead_letter_queue_name)

    # declare the queue
    channel.queue_declare(
        queue_name,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": dead_letter_queue_name,
        },
    )
    logger.info('Declared "%s" queue.', queue_name)

    # bind the queue
    channel.queue_bind(
        exchange=CA_TRADE_EXCHANGE,
        queue=queue_name,
        routing_key=routing_key,
    )
    logger.info(
        'Created a binding from "%s" to "%s" with routing key "%s".',
        CA_TRADE_EXCHANGE,
        queue_name,
        routing_key,
    )


@swpt_trade.command("create_chores_queue")
@with_appcontext
def create_chores_queue():  # pragma: no cover
    """Declare a RabbitMQ queue for trade' chores."""

    logger = logging.getLogger(__name__)
    queue_name = current_app.config["CHORES_BROKER_QUEUE"]
    dead_letter_queue_name = queue_name + ".XQ"
    broker_url = current_app.config["CHORES_BROKER_URL"]
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()

    # declare a corresponding dead-letter queue
    channel.queue_declare(dead_letter_queue_name, durable=True)
    logger.info('Declared "%s" dead-letter queue.', dead_letter_queue_name)

    # declare the queue
    channel.queue_declare(
        queue_name,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": dead_letter_queue_name,
        },
    )
    logger.info('Declared "%s" queue.', queue_name)


@swpt_trade.command("consume_messages")
@with_appcontext
@click.option("-u", "--url", type=str, help="The RabbitMQ connection URL.")
@click.option(
    "-q", "--queue", type=str, help="The name the queue to consume from."
)
@click.option(
    "-p", "--processes", type=int, help="The number of worker processes."
)
@click.option(
    "-t",
    "--threads",
    type=int,
    help="The number of threads running in each process.",
)
@click.option(
    "-s",
    "--prefetch-size",
    type=int,
    help="The prefetch window size in bytes.",
)
@click.option(
    "-c",
    "--prefetch-count",
    type=int,
    help="The prefetch window in terms of whole messages.",
)
def consume_messages(
    url, queue, processes, threads, prefetch_size, prefetch_count
):
    """Consume and process incoming Swaptacular Messaging Protocol
    messages.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_trade")

    * PROTOCOL_BROKER_PROCESSES (defalut 1)

    * PROTOCOL_BROKER_THREADS (defalut 1)

    * PROTOCOL_BROKER_PREFETCH_COUNT (default 1)

    * PROTOCOL_BROKER_PREFETCH_SIZE (default 0, meaning unlimited)

    """

    def _consume_messages(
        url, queue, threads, prefetch_size, prefetch_count
    ):  # pragma: no cover
        """Consume messages in a subprocess."""

        from swpt_trade.actors import SmpConsumer, TerminatedConsumtion
        from swpt_trade import create_app

        consumer = SmpConsumer(
            app=create_app(),
            config_prefix="PROTOCOL_BROKER",
            url=url,
            queue=queue,
            threads=threads,
            prefetch_size=prefetch_size,
            prefetch_count=prefetch_count,
        )
        for sig in HANDLED_SIGNALS:
            signal.signal(sig, consumer.stop)
        try_unblock_signals()

        pid = os.getpid()
        logger = logging.getLogger(__name__)
        logger.info("Worker with PID %i started processing messages.", pid)

        try:
            consumer.start()
        except TerminatedConsumtion:
            pass

        logger.info("Worker with PID %i stopped processing messages.", pid)

    spawn_worker_processes(
        processes=processes or current_app.config["PROTOCOL_BROKER_PROCESSES"],
        target=_consume_messages,
        url=url,
        queue=queue,
        threads=threads,
        prefetch_size=prefetch_size,
        prefetch_count=prefetch_count,
    )
    sys.exit(1)


@swpt_trade.command("consume_chore_messages")
@with_appcontext
@click.option("-u", "--url", type=str, help="The RabbitMQ connection URL.")
@click.option(
    "-q", "--queue", type=str, help="The name the queue to consume from."
)
@click.option(
    "-p", "--processes", type=int, help="The number of worker processes."
)
@click.option(
    "-t",
    "--threads",
    type=int,
    help="The number of threads running in each process.",
)
@click.option(
    "-s",
    "--prefetch-size",
    type=int,
    help="The prefetch window size in bytes.",
)
@click.option(
    "-c",
    "--prefetch-count",
    type=int,
    help="The prefetch window in terms of whole messages.",
)
def consume_chore_messages(
    url, queue, processes, threads, prefetch_size, prefetch_count
):
    """Consume and process chore messages.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * CHORES_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * CHORES_BROKER_QUEUE (defalut "swpt_trade_chores")

    * CHORES_BROKER_PROCESSES (defalut 1)

    * CHORES_BROKER_THREADS (defalut 1)

    * CHORES_BROKER_PREFETCH_COUNT (default 1)

    * CHORES_BROKER_PREFETCH_SIZE (default 0, meaning unlimited)

    """

    def _consume_chore_messages(
        url, queue, threads, prefetch_size, prefetch_count
    ):  # pragma: no cover
        from swpt_trade.chores import ChoresConsumer, TerminatedConsumtion
        from swpt_trade import create_app

        consumer = ChoresConsumer(
            app=create_app(),
            config_prefix="CHORES_BROKER",
            url=url,
            queue=queue,
            threads=threads,
            prefetch_size=prefetch_size,
            prefetch_count=prefetch_count,
        )
        for sig in HANDLED_SIGNALS:
            signal.signal(sig, consumer.stop)
        try_unblock_signals()

        pid = os.getpid()
        logger = logging.getLogger(__name__)
        logger.info("Worker with PID %i started processing messages.", pid)

        try:
            consumer.start()
        except TerminatedConsumtion:
            pass

        logger.info("Worker with PID %i stopped processing messages.", pid)

    spawn_worker_processes(
        processes=processes or current_app.config["CHORES_BROKER_PROCESSES"],
        target=_consume_chore_messages,
        url=url,
        queue=queue,
        threads=threads,
        prefetch_size=prefetch_size,
        prefetch_count=prefetch_count,
    )
    sys.exit(1)


@swpt_trade.command("flush_messages")
@with_appcontext
@click.option(
    "-p",
    "--processes",
    type=int,
    help=(
        "Then umber of worker processes."
        " If not specified, the value of the FLUSH_PROCESSES environment"
        " variable will be used, defaulting to 1 if empty."
    ),
)
@click.option(
    "-w",
    "--wait",
    type=float,
    help=(
        "Flush every FLOAT seconds."
        " If not specified, the value of the FLUSH_PERIOD environment"
        " variable will be used, defaulting to 2 seconds if empty."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
@click.argument("message_types", nargs=-1)
def flush_messages(
    message_types: list[str],
    processes: int,
    wait: float,
    quit_early: bool,
) -> None:
    """Send pending messages to the message broker.

    If a list of MESSAGE_TYPES is given, flushes only these types of
    messages. If no MESSAGE_TYPES are specified, flushes all messages.

    """
    logger = logging.getLogger(__name__)
    models_to_flush = get_models_to_flush(
        current_app.extensions["signalbus"], message_types
    )
    logger.info(
        "Started flushing %s.", ", ".join(m.__name__ for m in models_to_flush)
    )

    def _flush(
        models_to_flush: list[type[Model]],
        wait: float,
    ) -> None:  # pragma: no cover
        from swpt_trade import create_app

        app = create_app()
        stopped = False

        def stop(signum: Any = None, frame: Any = None) -> None:
            nonlocal stopped
            stopped = True

        for sig in HANDLED_SIGNALS:
            signal.signal(sig, stop)
        try_unblock_signals()

        with app.app_context():
            signalbus: SignalBus = current_app.extensions["signalbus"]
            while not stopped:
                started_at = time.time()
                try:
                    count = signalbus.flushmany(models_to_flush)
                except Exception:
                    logger.exception(
                        "Caught error while sending pending signals."
                    )
                    sys.exit(1)

                if count > 0:
                    logger.info(
                        "%i signals have been successfully processed.", count
                    )
                else:
                    logger.debug("0 signals have been processed.")

                if quit_early:
                    break
                time.sleep(max(0.0, wait + started_at - time.time()))

    spawn_worker_processes(
        processes=(
            processes
            if processes is not None
            else current_app.config["FLUSH_PROCESSES"]
        ),
        target=_flush,
        models_to_flush=models_to_flush,
        wait=(
            wait if wait is not None else current_app.config["FLUSH_PERIOD"]
        ),
    )
    sys.exit(1)


@swpt_trade.command("roll_turns")
@with_appcontext
@click.option(
    "-p",
    "--period",
    type=str,
    help=(
        "Start a new turn every TEXT seconds."
        " If not specified, the value of the TURN_PERIOD environment"
        " variable will be used, defaulting to 1 day if empty. A unit"
        " can also be included in the value. For example, 10m would be"
        " equivalent to 600 seconds."
    ),
)
@click.option(
    "-o",
    "--period-offset",
    type=str,
    help=(
        "Start each turn TEXT seconds after the start of each period."
        " If not specified, the value of the TURN_PERIOD_OFFSET environment"
        " variable will be used, defaulting to 0 if empty. A unit can"
        " also be included in the value. For example, if the turn period"
        " is 1d, and the turn period offset is 1h, new turns will be"
        " started every day at 1:00am UTC time."
    ),
)
@click.option(
    "-c",
    "--check-interval",
    type=str,
    help=(
        "The process will wake up every TEXT seconds to check whether"
        " a new turn has to be started, or an already started turn"
        " has to be advanced. If not specified, the value of the"
        " TURN_CHECK_INTERVAL environment variable will be used,"
        " defaulting to 1 minute if empty. A unit can also be included"
        " in the value. For example, 2m would be equivalent to 120 seconds."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def roll_turns(period, period_offset, check_interval, quit_early):
    """Run a process that starts new turns, and advances started
    turns.

    Every turn consists of several phases. When one phase is
    completed, the turn advances to the next phase. The durations of
    phases 1 and 2 are controlled by the environment variables
    TURN_PHASE1_DURATION and TURN_PHASE2_DURATION. The maximum
    duration of the commit period is controlled by the
    TURN_MAX_COMMIT_PERIOD environment variable. (Note that time units
    can also be included in values of these variables.)

    Another important environment variables which control the way
    trading turns work are: BASE_DEBTOR_INFO_LOCATOR, BASE_DEBTOR_ID,
    MAX_DISTANCE_TO_BASE, MIN_TRADE_AMOUNT.
    """
    from swpt_trade.utils import parse_timedelta

    c = current_app.config
    period = parse_timedelta(period or c["TURN_PERIOD"])
    period_offset = parse_timedelta(period_offset or c["TURN_PERIOD_OFFSET"])
    check_interval = parse_timedelta(
        check_interval or c["TURN_CHECK_INTERVAL"]
    )
    phase1_duration = parse_timedelta(c["TURN_PHASE1_DURATION"])
    phase2_duration = parse_timedelta(c["TURN_PHASE2_DURATION"])
    max_commit_period = parse_timedelta(c["TURN_MAX_COMMIT_PERIOD"])

    logger = logging.getLogger(__name__)
    logger.info("Started rolling turns.")

    while True:
        logger.info("Trying to start a new turn and advance started turns.")
        check_began_at = datetime.now(tz=timezone.utc)
        started_turns = procedures.start_new_turn_if_possible(
            turn_period=period,
            turn_period_offset=period_offset,
            phase1_duration=phase1_duration,
            base_debtor_info_locator=c["BASE_DEBTOR_INFO_LOCATOR"],
            base_debtor_id=c["BASE_DEBTOR_ID"],
            max_distance_to_base=c["MAX_DISTANCE_TO_BASE"],
            min_trade_amount=c["MIN_TRADE_AMOUNT"],
        )
        for turn in started_turns:
            phase = turn.phase
            if phase == 1 and turn.phase_deadline < check_began_at:
                procedures.try_to_advance_turn_to_phase2(
                    turn_id=turn.turn_id,
                    phase2_duration=phase2_duration,
                    max_commit_period=max_commit_period,
                )
            elif phase == 2 and turn.phase_deadline < check_began_at:
                try_to_advance_turn_to_phase3(turn.turn_id)
            elif phase == 3:
                procedures.try_to_advance_turn_to_phase4(turn.turn_id)

        elapsed_time = datetime.now(tz=timezone.utc) - check_began_at
        wait_seconds = (check_interval - elapsed_time).total_seconds()

        if quit_early:
            break
        if wait_seconds > 0.0:  # pragma: no cover
            time.sleep(wait_seconds)
