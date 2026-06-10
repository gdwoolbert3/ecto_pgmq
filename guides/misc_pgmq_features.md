# Miscellaneous PGMQ Features

TODO(Gordon) - Fill out this doc

## Headers

## Partitioning

TODO(Gordon) - update this
PGMQ supports partitioning both queues and archives.

The [pg_partman extension](https://github.com/pgpartman/pg_partman) must be
available in order to use partitioning.

For more information about partitioning, see the
[PGMQ docs](https://github.com/pgmq/pgmq/tree/main?tab=readme-ov-file#partitioned-queues).

## Polling

TODO(Gordon) - update this
PGMQ supports Postgres server-side polling during read operations. Reading
with a poll can be used to reduce network round trips if there is a good
chance that demand can be satisfied in a short time **BUT** doing so utilizes
a connection for the duration of the read operation. As such, polling should
be avoided in situations where the DB connection pool is a bottleneck.
