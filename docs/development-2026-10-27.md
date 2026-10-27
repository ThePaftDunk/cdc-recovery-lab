# Development session: real Scala CDC recovery

Date: 27 October 2026. Branch: `portfolio/live-cdc-recovery`, based on `main`. The reconciliation PR remains independent.

## Goal

Validate the Scala worker with PostgreSQL, Debezium and Kafka instead of extending the TypeScript diagnostics project again. Kafka Ops MCP remains in TypeScript, as Shubham requested. This session adds a Scala integration runner and CI evidence; it does not start a language migration.

## Observed evidence

`./scripts/sbtw test 'Test / compile' assembly` passed all eight existing recovery tests and built the worker jar. `DOCKER_CONTEXT=colima-portfolio-lab ./scripts/sbtw liveCheck` passed eight live phases:

1. Two initial rows reached Kafka from an actual Debezium pgoutput snapshot.
2. A fault before PostgreSQL commit rolled back both projection and checkpoint.
3. A sink record remained committed after close while Kafka had no acknowledgement for the group.
4. A fresh packaged worker resumed from the PostgreSQL checkpoint and applied only the remaining snapshot event.
5. Actual source inserts, updates and deletes converged with independent target SQL reads.
6. A poison record was quarantined at its real Kafka offset and a later WAL change still progressed.
7. After a deliberate administrative rewind of Kafka's group offset, the restarted worker used the higher database checkpoint and processed only one new source update.
8. Moving the broker's retained start beyond the database checkpoint caused a fresh worker to fail closed, preserving target state and Kafka acknowledgement.

The runtime was Scala 2.13.16, OpenJDK 24.0.2 and sbt 1.11.5 on macOS/Apple silicon. The owned Colima profile used 2 CPUs, 3 GiB RAM and no host mounts. JVM 24 emits existing sbt native/Unsafe warnings; the assertions passed. CI targets JDK 21 separately.

Images are locked to these digests, also recorded in the generated report:

- PostgreSQL 16.6: `sha256:557fea37a744d5f4c8faab304b0a90858b53ab119735a88c131fd19dab802f36`
- Kafka 4.0.2: `sha256:836cafdad9f4825880d7cf1d5a21202915ae2527bd0ef1c3600c526ed7814d1f`
- Debezium Connect 3.0: `sha256:5547ddf063d6c20c56b56d660eb6bc449b180e6a70b2d69fc981629672b1f213`

The first attempt passed six phases but the broker rejected a group-offset alteration while a terminated worker's membership was still present. The runner now waits for the group to be empty and uses an explicit test-only session timeout. The complete rerun passed. This is a test-control precondition, not a production group-draining guarantee.

## Walkthrough for Shubham

Run `./scripts/sbtw liveCheck` with a Docker engine, then inspect `target/live-cdc-report.json`. Explain why the sink's next offset is authoritative, why the Kafka acknowledgement follows PostgreSQL commit, and how a known commit/ack gap is constructed rather than hoping a random kill lands inside it. Show how source/target SQL equality, exact worker metrics and group offsets establish different parts of the result.

The [ADR](adr-live-recovery.md) records alternatives and limits. A single-partition successful recovery does not establish concurrent ownership fencing or a transaction across Kafka and PostgreSQL.

## Next experiment

Test a bounded rebalance/ownership case or define a repeatable source snapshot boundary before adding concurrency. Containerized worker deployment, production authentication, failure of broker/database hosts and load measurements remain separate work.
