# Recover a lagging or failed pipeline

1. Check `/health/live`, `/health/ready`, and `/metrics`. Read the first worker exception before restarting repeatedly.
2. Compare `checkpoints.next_offset` with the group's committed next offset and the broker's beginning/end offsets for every assigned partition. The sink checkpoint is the application authority.
3. If the sink is unavailable, fix connectivity or database capacity. A failed JDBC transaction must leave projection, quarantine, and checkpoint unchanged.
4. If progress is increasing but lag grows, measure processing rate, database transaction time, and Kafka commit time. Do not infer a root cause from one lag snapshot.
5. If the checkpoint is before retention or after the log end, preserve evidence and rebuild from a verified source snapshot in a separate target. Do not automatically reset offsets against a partly populated sink.
6. Inspect quarantine provenance and repair invalid changes at new source offsets. Reusing a quarantined offset will be a duplicate.
7. Restart and verify the target contents and checkpoint progression, then compare source and target keys. A passing readiness probe alone does not establish data correctness.

Useful local checks:

```sh
./scripts/sbtw 'run inspect'
curl http://localhost:8081/health/ready
curl http://localhost:8081/metrics
docker compose logs --tail 100 worker connect
```

Production additions: replication-slot/WAL growth alerting, checkpoint age, restart frequency, partition fencing, reconciliation, schema compatibility, quarantine retention, and load tests against the actual PostgreSQL isolation configuration.
