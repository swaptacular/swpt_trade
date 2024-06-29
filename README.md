Swaptacular "Circular Trade" reference implementation
=====================================================

This project implements a "Circular Trade" service for [Swaptacular].
The deliverables are two [docker images]: the *app-image*, and the
*swagger-ui-image*. Both images are generated from the project's
[Dockerfile](../master/Dockerfile).

* The `app-image` implements 3 types of servers that the "Circular
  Trade" service consists of:

  1. **One "solver" server.**
  
  This server periodically gathers currencies information, together
  with buy and sell offers, and analyses them so as to propose a list
  of transactions that will be beneficial for all the participants.

  2. **One or more "worker" servers.**

  These servers work together to collect all the information that the
  "solver" server needs, and to actually perform the transactions
  proposed by the "solver" server. Each "worker" server is responsible
  for a different [database shard].

  3. **An optional "admin API" web server.**

  This server implements a Web-API that provides various
  administrative tools, useful for the continued proper functioning of
  the "Circular Trade" service.

* The `swagger-ui-image` is a simple [Swagger UI] client for the admin
  Web API, mainly useful for testing.

**Note:** This implementation uses [JSON Serialization for the
Swaptacular Messaging Protocol].


Dependencies
------------

Containers started from the *app-image* must have access to the
following servers:

1. One [PostgreSQL] server instance, which stores the solver server's
   data.

2. One PostgreSQL server instance **for each worker server**.

3. A [RabbitMQ] server instance, which acts as broker for [Swaptacular
   Messaging Protocol] (SMP) messages.

   The following [RabbitMQ exchanges] must be configured on the broker
   instance:

   - **`creditors_out`**: For messages that must be sent to accounting
     authorities. The routing key will represent the debtor ID as
     hexadecimal (lowercase). For example, for debtor ID equal to 10, the
     routing key will be "00.00.00.00.00.00.00.0a".

   - **`to_trade`**: For messages that must be processed by one of the
     worker servers. The routing key will represent the highest 24
     bits of the MD5 digest of the sharding key. For example, if the
     sharding key for a given message type is the creditor ID, and the
     creditor ID is 123, the routing key will be
     "1.1.1.1.1.1.0.0.0.0.0.1.0.0.0.0.0.1.1.0.0.0.1.1". This allows
     different messages to be handled by different worker servers
     (sharding).

     The following types of messages will be published on this
     exchange:

     * Incoming SMP messages related to *collector accounts*. (To do
       their jobs, worker servers create and use system accounts,
       called "collector accounts", which act as distribution hubs for
       money.)

     * Incoming SMP messages concerning transfers with *"agent"*
       coordinator type. (The "agent" coordinator type is reserved for
       transfers initiated by the "Circular Trade" service, on behalf
       of users.)

     * Policy, ledger, and flag update notifications, sent by the
       subsystem that is responsible for managing users' accounts.
       (See the ["Creditors Agent" reference implementation].)

     * Internal messages. (To do their jobs, worker servers will send
       messages to each other. Even when there is only one worker
       server, it will use the `to_trade` exchange to send messages to
       itself.)

   Also, **for each worker server** one [RabbitMQ queue] must be
   configured on the broker instance, so that all messages published
   on the `to_trade` exchange, are routed to one of these queues
   (determined by the rouging key).

   **Note:** If you execute the "configure" command (see below), with
   the environment variable `SETUP_RABBITMQ_BINDINGS` set to `yes`, an
   attempt will be made to automatically setup all the required
   RabbitMQ queues, exchanges, and the bindings between them.

4. An optional [OAuth 2.0] authorization server, which authorizes
   clients' requests to the *admin API*. There is a plethora of
   popular Oauth 2.0 server implementations. Normally, they maintain
   their own user database, and go together with UI for user
   registration, login, and authorization consent.

   Also, to increase security and performance, it is highly
   recommended that you configure HTTP reverse-proxy server(s) (like
   [nginx]) between the admin clients and the "admin API".


Configuration
-------------

The behavior of the running container can be tuned with environment
variables. Here are the most important settings with some random
example values:

## Configuring for the "solver" server

```shell
# The creditors agent will be responsible only for creditor IDs
# between "$MIN_CREDITOR_ID" and "$MAX_CREDITOR_ID". This can be
# passed as a decimal number (like "4294967296"), or a
# hexadecimal number (like "0x100000000"). Numbers between
# 0x8000000000000000 and 0xffffffffffffffff will be automatically
# converted to their corresponding two's complement negative
# numbers.
MIN_CREDITOR_ID=4294967296
MAX_CREDITOR_ID=8589934591

# To improve the security of their accounts, creditors may configure
# PINs (Personal Identification Numbers). The PINs will be stored in
# the database encrypted. This variable determines the encryption key
# (default ""). If a malicious attacker knows the encryption key, and
# has a copy of the database, he/she will be able to decrypt all the
# PINs. If you lose this encryption key, the users' PINs will not be
# verified correctly, until each user configures a new PIN.
PIN_PROTECTION_SECRET=some-long-string-that-must-be-kept-secret

# The specified number of processes ("$WEBSERVER_PROCESSES") will be
# spawned to handle "Payments Web API" requests (default 1),
# each process will run "$WEBSERVER_THREADS" threads in parallel
# (default 3). The container will listen for "Payments Web API"
# requests on port "$WEBSERVER_PORT" (default 8080).
WEBSERVER_PROCESSES=2
WEBSERVER_THREADS=10
WEBSERVER_PORT=8003

# Requests to the "Payments Web API" are protected by an OAuth
# 2.0 authorization server. With every request, the client (a Web
# browser, for example) presents a token, and to verify the validity
# of the token, internally, a request is made to the OAuth 2.0
# authorization server. This is called "token introspection". This
# variable sets the URL at which internal token introspection requests
# will be sent.
#
# NOTE: The response to the "token introspection" request will contain
# a "username" field. The OAuth 2.0 authorization server must be
# configured to return usernames that match one of the following
# regular expressions: ^creditors-superuser$, ^creditors-supervisor$,
# ^creditors:([0-9]+)$. The "creditors-superuser" account will be
# allowed to do everything; the "creditors-supervisor" account will be
# allowed to view creditors' data, and to create new creditors; the
# "creditors:<CREDITOR_ID>" accounts will only be allowed access to
# the creditor with the specified <CREDITOR_ID> (an unsigned 64-bit
# integer).
OAUTH2_INTROSPECT_URL=http://localhost:4445/oauth2/introspect

# Connection string for a PostgreSQL database server to connect to.
POSTGRES_URL=postgresql+psycopg://swpt_creditors:swpt_creditors@localhost:5435/test

# Connection string for a Redis database server to connect to.
REDIS_URL=redis://localhost:6380/0?health_check_interval=30

# Parameters for the communication with the RabbitMQ server which is
# responsible for brokering SMP messages. The container will connect
# to "$PROTOCOL_BROKER_URL" (default
# "amqp://guest:guest@localhost:5672"), will consume messages from the
# queue named "$PROTOCOL_BROKER_QUEUE" (default "swpt_creditors"),
# prefetching at most "$PROTOCOL_BROKER_PREFETCH_COUNT" messages at
# once (default 1). The specified number of processes
# ("$PROTOCOL_BROKER_PROCESSES") will be spawned to consume and
# process messages (default 1), each process will run
# "$PROTOCOL_BROKER_THREADS" threads in parallel (default 1). Note
# that PROTOCOL_BROKER_PROCESSES can be set to 0, in which case, the
# container will not consume any messages from the queue.
PROTOCOL_BROKER_URL=amqp://guest:guest@localhost:5672
PROTOCOL_BROKER_QUEUE=swpt_creditors
PROTOCOL_BROKER_PROCESSES=1
PROTOCOL_BROKER_THREADS=3
PROTOCOL_BROKER_PREFETCH_COUNT=10

# The binding key with which the "$PROTOCOL_BROKER_QUEUE"
# RabbitMQ queue is bound to the incoming messages' topic
# exchange (default "#"). The binding key must consist of zero or
# more 0s or 1s, separated by dots, ending with a hash symbol.
# For example: "0.1.#", "1.#", or "#".
PROTOCOL_BROKER_QUEUE_ROUTING_KEY=#

# All outgoing Swaptacular Messaging Protocol messages are first
# recorded in the PostgreSQL database, and then are "fulshed" to
# the RabbitMQ message broker. The specified number of
# processes ("$FLUSH_PROCESSES") will be spawned to flush
# messages (default 1). Note that FLUSH_PROCESSES can be set to
# 0, in which case, the container will not flush any messages.
# The "$FLUSH_PERIOD" value specifies the number of seconds to
# wait between two sequential flushes (default 2).
FLUSH_PROCESSES=2
FLUSH_PERIOD=1.5

# The processing of incoming events consists of several stages. The
# following configuration variables control the number of worker
# threads that will be involved on each respective stage (default
# 1). You must set this to a reasonable value, and increase it when
# you start experiencing problems with performance.
PROCESS_LOG_ADDITIONS_THREADS=10
PROCESS_LEDGER_UPDATES_THREADS=10

# Set this to "true" after splitting a parent database shard into
# two children shards. You may set this back to "false", once all
# left-over records from the parent have been deleted from the
# child shard.
DELETE_PARENT_SHARD_RECORDS=false

# Set the minimum level of severity for log messages ("info",
# "warning", or "error"). The default is "warning".
APP_LOG_LEVEL=info

# Set format for log messages ("text" or "json"). The default is
# "text".
APP_LOG_FORMAT=text
```

For more configuration options, check the
[development.env](../master/development.env) file.


[Swaptacular]: https://swaptacular.github.io/overview
[docker images]: https://www.geeksforgeeks.org/what-is-docker-images/
[database shard]: https://en.wikipedia.org/wiki/Shard_(database_architecture)
[Swagger UI]: https://swagger.io/tools/swagger-ui/
[JSON Serialization for the Swaptacular Messaging Protocol]: https://github.com/swaptacular/swpt_accounts/blob/master/protocol-json.rst
[PostgreSQL]: https://www.postgresql.org/
[RabbitMQ]: https://www.rabbitmq.com/
[RabbitMQ queue]: https://www.cloudamqp.com/blog/part1-rabbitmq-for-beginners-what-is-rabbitmq.html
[RabbitMQ exchanges]: https://www.cloudamqp.com/blog/part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html
[Swaptacular Messaging Protocol]: https://github.com/swaptacular/swpt_accounts/blob/master/protocol.rst
["Creditors Agent" reference implementation]: https://github.com/swaptacular/swpt_creditors
[OAuth 2.0]: https://oauth.net/2/
[nginx]: https://en.wikipedia.org/wiki/Nginx
