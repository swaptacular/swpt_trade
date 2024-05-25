import logging
import click
from datetime import timedelta
from flask import current_app
from flask.cli import with_appcontext
from swpt_pythonlib.utils import ShardingRealm
from swpt_pythonlib.multiproc_utils import ThreadPoolProcessor
from swpt_trade.utils import u16_to_i16
from swpt_trade import procedures
from .common import swpt_trade


@swpt_trade.command("process_pristine_collectors")
@with_appcontext
@click.option(
    "-t", "--threads", type=int, help="The number of worker threads."
)
@click.option(
    "-w",
    "--wait",
    type=float,
    help=(
        "The minimal number of seconds between"
        " the queries to obtain pristine collector accounts."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def process_pristine_collectors(threads, wait, quit_early):
    """Process all pristine collector accounts.

    If --threads is not specified, the value of the configuration
    variable PROCESS_PRISTINE_COLLECTORS_THREADS is taken. If it is
    not set, the default number of threads is 1.

    If --wait is not specified, the value of the configuration
    variable APP_PROCESS_PRISTINE_COLLECTORS_WAIT is taken. If it is
    not set, the default number of seconds is 60.
    """

    # TODO: Consider allowing load-sharing between multiple processes
    #       or containers. This may also be true for the other
    #       "process_*" CLI commands. A possible way to do this is to
    #       separate the `args collection` in multiple buckets,
    #       assigning a dedicated process/container for each bucket.
    #       Note that this would makes sense only if the load is
    #       CPU-bound, which is unlikely, especially if we
    #       re-implement the logic in stored procedures.

    threads = threads or current_app.config[
        "PROCESS_PRISTINE_COLLECTORS_THREADS"
    ]
    wait = (
        wait
        if wait is not None
        else current_app.config["APP_PROCESS_PRISTINE_COLLECTORS_WAIT"]
    )
    max_count = current_app.config["APP_PROCESS_PRISTINE_COLLECTORS_MAX_COUNT"]
    max_postponement = timedelta(
        days=current_app.config["APP_EXTREME_MESSAGE_DELAY_DAYS"]
    )
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    hash_prefix = u16_to_i16(sharding_realm.realm >> 16)
    hash_mask = u16_to_i16(sharding_realm.realm_mask >> 16)

    def get_args_collection():
        return procedures.get_pristine_collectors(
            hash_mask=hash_mask,
            hash_prefix=hash_prefix,
            max_count=max_count,
        )

    def process_pristine_collector(debtor_id, collector_id):
        assert sharding_realm.match(collector_id)
        procedures.configure_worker_account(
            debtor_id=debtor_id,
            collector_id=collector_id,
            max_postponement=max_postponement,
        )
        procedures.mark_requested_collector(
            debtor_id=debtor_id, collector_id=collector_id
        )

    logger = logging.getLogger(__name__)
    logger.info("Started pristine collector accounts processor.")

    ThreadPoolProcessor(
        threads,
        get_args_collection=get_args_collection,
        process_func=process_pristine_collector,
        wait_seconds=wait,
        max_count=max_count,
    ).run(quit_early=quit_early)
