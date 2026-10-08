# Development record: checking target contents

Date: 9 October 2026.

## Problem selected

The first release established checkpoint/replay behavior. It did not independently check whether target contents matched an expected source export. A correct-looking checkpoint alone is insufficient evidence of convergence.

## Change

Added `reconcile`, a read-only bounded comparison with missing, unexpected and changed counts. A separate JDBC reader keeps diagnosis apart from sink writes. Reports contain keys and field names, with deterministic sample limits; they omit row bodies. A match exits 0 and detected drift exits 2.

The decision and alternatives are in [the reconciliation ADR](adr-reconciliation.md). The first version rejects numeric values because the current JSON parser can hide numeric precision differences. It also fails above 10,000 rows per side. These are explicit correctness/scope decisions, not claims of complete production reconciliation.

## Validation

15 Scala tests pass: 8 recovery tests and 7 reconciliation tests. The assembled jar is exercised by a CLI test using an isolated temporary database, a matching export and an intentionally different tier. It verifies exit codes and report contents. Tests show the JDBC reader preserves checkpoints/quarantine and does not create a missing H2 database.

The PostgreSQL/Kafka live integration and a coordinated source/target snapshot remain unverified. This change does not claim to detect drift between moving live snapshots.

## Walkthrough for Shubham

Run the CLI test, inspect the matching and differing cases, and explain why a checkpoint cannot prove target contents. Review why the comparison does not repair data, why absence differs from JSON null, why numbers are rejected, and how a chunked live reconciler would need a shared boundary.

## Next candidate

Run the actual PostgreSQL/Debezium/Kafka recovery path and record its source boundary before extending reconciliation size or concurrency. The development record describes this session's work; it is not a historical workplace project claim.
