# ADR: commit the projection and checkpoint together

Status: accepted for this portfolio implementation.

If a sink commits and the process fails before committing the Kafka offset, the next owner sees the same record. If the Kafka offset commits first and the sink fails, data disappears from the projection. The sink must own the durable progress decision.

We store `(topic, partition, next_offset)` in the same database transaction as an upsert/delete or a quarantine entry. Checkpoint rows are locked during updates. Replayed offsets below `next_offset` are skipped. On partition assignment the consumer seeks to the durable sink checkpoint, verifies retention bounds, and processes in source order. PostgreSQL keeps the application and progress durable without a cross-system transaction coordinator.

Quarantine-and-advance is an explicit policy: a poison record does not block the entire partition, but the projection may omit that change. The record and its provenance remain available for operator repair. A database error is not quarantined; it rolls back and stops the worker so source data is not acknowledged.

Alternatives considered: commit Kafka first (data-loss window), idempotency receipts for every event (unbounded receipt storage), or Kafka transactions alone (do not cover JDBC writes). The compact per-partition checkpoint fits a strictly ordered consumer. Out-of-order processing would invalidate that simplification.

This design does not promise complete historical reconstruction after retention loss. It also does not establish Kafka topic lineage, multiple-table key namespaces, or ownership fencing for arbitrary parallel sink jobs. Those are required extensions before production use.
