# ADR: verify recovery across real PostgreSQL and Kafka boundaries

Status: accepted for the disposable integration runner, 27 October 2026.

## Problem

The H2 tests exercise transaction and offset logic, but they do not establish PostgreSQL JDBC behavior, Debezium message compatibility, real Kafka bounds or the packaged worker's assignment/restart path. The documented Compose stack had not been executed. Recovery needed evidence beyond a passing in-memory test or a healthy process.

## Decision

Add an explicit `liveCheck` command implemented in Scala. It owns a private Docker network, source/sink PostgreSQL containers, one Kafka broker and Debezium Connect, all pinned by image digest. Reuse `ops/source.sql` and `ops/connector.json`, replacing only the generated database password. Start the assembled Scala worker as a separate JVM on the host, so it executes its actual production `kafka` mode while the Docker engine can remain in a VM without host mounts.

Use the same Scala/JDBC sink against PostgreSQL for a controlled pre-commit failure and a durable committed record whose Kafka acknowledgement is withheld. Start the packaged worker and verify that it seeks the database checkpoint: only the remaining snapshot event is applied, with no duplicate events counted. Validate insert/update/delete changes from actual PostgreSQL WAL and compare the table with the target using independent SQL reads.

Inject one synthetic invalid Kafka value, verify its quarantine offset and subsequent progress, then stop the worker. Wait until Kafka reports the consumer group empty before deliberately rewinding its committed offset. Restart the real worker and emit one new source change; only that change may be processed. A ten-second session timeout is configured only for the test worker to bound coordinator membership expiry.

Finally, stop the worker, append another synthetic record and move Kafka's retained start beyond the durable database checkpoint through the broker's `DeleteRecords` API. A new worker JVM must exit with the checkpoint-range error, leave the target unchanged and acknowledge nothing new.

## Alternatives and tradeoffs

- H2 fixtures remain the fast default and cover more combinations, but cannot prove these external-system behaviors.
- Testing only a Kafka producer/consumer against a database would miss the actual Debezium snapshot and WAL envelopes. The live check includes the connector.
- Running the worker only inside the harness would miss its packaged entry point, configuration and assignment callback. Fresh jar subprocesses exercise those paths.
- Randomly killing a worker might miss the commit/acknowledgement gap. Construct the real durable state deterministically, then exercise recovery through actual worker processes. This is not a measured power-loss experiment.
- A shared broker/database would require careful permissions and risk interference. The runner accepts no external target argument and removes only its UUID-named resources.

The initial live run confirmed six phases but administrative offset alteration failed while Kafka still considered the stopped worker a group member. Waiting for the group to become empty establishes the control operation's precondition; it is not evidence of a production shutdown/draining SLO.

## Limits

The experiment uses one table, one Kafka partition, one active sink writer and generated local credentials. It does not establish multi-worker fencing, rebalance races, production TLS/ACL setup, replication-slot disk budgets, failover, throughput or end-to-end atomicity spanning Kafka and PostgreSQL. Initial snapshot consistency is provided by the tested Debezium configuration; the SQL comparisons occur at controlled quiescent checkpoints and are not a general live reconciliation guarantee.

The Compose file and Docker worker image remain distinct deployment surfaces. The runner reuses their source/connector inputs but does not claim to validate their orchestration or image build. Debezium's [PostgreSQL connector documentation](https://debezium.io/documentation/reference/3.0/connectors/postgresql.html) describes the snapshot and pgoutput/WAL path exercised here.
