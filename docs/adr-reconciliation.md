# ADR: reconciliation diagnoses drift and leaves recovery to the operator

Date: 9 October 2026. Status: accepted for the first bounded implementation.

Checkpoint progress proves records were processed, but does not independently establish that target contents match the intended source state. Introduce a read-only comparison against an independently supplied fixed JSON export.

Report missing target keys, unexpected target keys and changed top-level fields. Return process exit code 2 for a valid comparison with differences, 0 for a match, and a normal command failure for invalid input or a failed read. Include full counts and at most 20 deterministic examples per category. Do not include row bodies. Object field order is irrelevant; array order and absent-versus-null fields matter.

Use a separate JDBC reader with only a SELECT statement, a read-only transaction and a rollback on close. Do not initialize tables, create missing H2 databases, change checkpoints, read quarantine payloads or automatically repair the target. A differing source export might reflect concurrent progress rather than corruption; automatic repair could overwrite valid new changes.

Bound this first version to 10,000 rows per side and a 16 MiB export. Fail if the target exceeds the row cap rather than reporting a partial comparison as successful. Support string, boolean, null, object and array values. Reject numeric fields because the current JSON library parses numbers as Double and can hide precision loss. Exact numeric canonicalization and chunked comparisons are separate future decisions.

A comparable source/target snapshot is the caller's responsibility. Pause writes and drain the worker, or use an export captured at a known boundary with a stopped target. A live comparison at unrelated times does not prove convergence. The CLI checks a single table and the complete target projection; comparing a partial export produces unexpected-key results by design.

Alternatives: only compare row counts (misses changed contents and swapped keys); automatically reset/replay offsets (may not fix drift and can lose retained history); compare full payloads in reports (unnecessary exposure); unbounded in-memory comparison (unsuitable for large pipelines).

Next experiment: real PostgreSQL/Debezium/Kafka integration, then chunked reconciliation with a documented snapshot boundary. The current H2 tests validate reader non-mutation and the comparison contract; they do not establish live PostgreSQL snapshot coordination.
