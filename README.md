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


[Swaptacular]: https://swaptacular.github.io/overview
[docker images]: https://www.geeksforgeeks.org/what-is-docker-images/
[database shard]: https://en.wikipedia.org/wiki/Shard_(database_architecture)
[Swagger UI]: https://swagger.io/tools/swagger-ui/
