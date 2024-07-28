import logging
import pika
from flask import current_app
from flask.cli import with_appcontext
from .common import swpt_trade

# TODO: Consider implementing a CLI command which extracts trading
# policies from the "swpt_creditors" microservice via its admin Web
# API, and loads them into the "trading policies" table. This CLI
# command is intended to be run only once at the beginning, to
# synchronize the swpt_trade's database with the swpt_creditors's
# database.


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

    from swpt_trade.extensions import (
        CREDITORS_IN_EXCHANGE,
        CREDITORS_OUT_EXCHANGE,
        TO_TRADE_EXCHANGE,
    )
    CA_LOOPBACK_FILTER_EXCHANGE = "ca.loopback_filter"

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
        CA_LOOPBACK_FILTER_EXCHANGE, exchange_type="headers", durable=True
    )
    channel.exchange_declare(
        CREDITORS_OUT_EXCHANGE,
        exchange_type="topic",
        durable=True,
        arguments={"alternate-exchange": CA_LOOPBACK_FILTER_EXCHANGE},
    )
    channel.exchange_declare(
        TO_TRADE_EXCHANGE, exchange_type="topic", durable=True
    )

    channel.exchange_bind(
        source=CREDITORS_IN_EXCHANGE,
        destination=TO_TRADE_EXCHANGE,
        arguments={
            "x-match": "all",
            "ca-trade": True,
        },
    )
    logger.info(
        'Created a binding from "%s" to the "%s" exchange.',
        CREDITORS_IN_EXCHANGE,
        TO_TRADE_EXCHANGE,
    )

    # Declare a queue and a corresponding dead-letter queue.
    #
    # TODO: It would probably be better to use a "stream" instead of
    #       classic queues here, given that we have figured out how to
    #       do the stream offset tracking. This would allow for
    #       high-availability. Using a "quorum" queues here is almost
    #       certainly not a good idea, because quorum queues consume
    #       lots of memory when there are lots of messages in the
    #       queue, which should be expected.
    channel.queue_declare(dead_letter_queue_name, durable=True)
    logger.info('Declared "%s" dead-letter queue.', dead_letter_queue_name)

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
        exchange=TO_TRADE_EXCHANGE,
        queue=queue_name,
        routing_key=routing_key,
    )
    logger.info(
        'Created a binding from "%s" to "%s" with routing key "%s".',
        TO_TRADE_EXCHANGE,
        queue_name,
        routing_key,
    )
