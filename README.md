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

1. One [PostgreSQL] server instance, which stores *the solver server's
   data*.

2. One PostgreSQL server instance **for each worker server**.

3. [RabbitMQ] server instance, which acts as broker for [Swaptacular
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

     * Incoming SMP messages related to the *collector accounts*. (To
       do its job, the "Circular Trade" service creates and uses
       system accounts, called "collector accounts", which act as
       distribution hubs for money.)

     * Incoming SMP messages concerning transfers with "agent"
       coordinator type. (The "agent" coordinator type is reserved for
       transfers initiated by the "Circular Trade" service on behalf
       of users.)

     * Internal "Circular Trade" messages. (To do their jobs, worker
       servers will send messages to each other. Even when there is
       only one worker server, it will use the `to_trade` exchange to
       send messages to itself.)

   Also, **for each worker server** one [RabbitMQ queue] must be
   configured on the broker instance, so that all messages published
   on the `to_trade` exchange, are routed to one of these queues
   (determined by the rouging key).

   **Note:** If you execute the "configure" command (see below), with
   the environment variable `SETUP_RABBITMQ_BINDINGS` set to `yes`, an
   attempt will be made to automatically setup all the required
   RabbitMQ queues, exchanges, and the bindings between them.

4. Optional [OAuth 2.0] authorization server, which authorizes
   clients' requests to the [Admin Web API]. There is a plethora of
   popular Oauth 2.0 server implementations. Normally, they maintain
   their own user database, and go together with UI for user
   registration, login, and authorization consent.

To increase security and performance, it is highly recommended that
you configure HTTP reverse-proxy server(s) (like [nginx]) between your
clients and your "Payments Web API". In addition, this approach allows
different creditors to be located on different database servers
(sharding).


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
[OAuth 2.0]: https://oauth.net/2/
