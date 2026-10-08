package portfolio.cdc

import java.nio.file.Files

class RecoverySuite extends munit.FunSuite {
  private def sink(): TransactionalSink = new TransactionalSink("jdbc:h2:mem:" + java.util.UUID.randomUUID() + ";MODE=PostgreSQL", "sa")
  private def put(offset: Long, tier: String, partition: Int = 0): Record = Record("customers", partition, offset, None, Some(s"""{"op":"u","after":{"id":"a-$partition","tier":"$tier"}}"""))
  test("failure before commit rolls back target and checkpoint") {
    val db = sink()
    try {
      db.process(put(0, "bronze"))
      intercept[IllegalStateException](db.process(put(1, "gold"), () => throw new IllegalStateException("crash")))
      assertEquals(db.nextOffset("customers", 0), Some(1L))
      assertEquals(db.snapshot()("projection")(0)("body")("tier").str, "bronze")
      assertEquals(db.process(put(1, "gold")), Applied)
    } finally db.close()
  }
  test("commit before ack: persistent restart rejects replay") {
    val directory = Files.createTempDirectory("cdc-recovery-test")
    val url = "jdbc:h2:file:" + directory.resolve("sink") + ";MODE=PostgreSQL"
    val first = new TransactionalSink(url, "sa"); first.process(put(0, "gold")); first.close()
    val second = new TransactionalSink(url, "sa")
    try { assertEquals(second.process(put(0, "bronze")), Duplicate); assertEquals(second.snapshot()("projection")(0)("body")("tier").str, "gold") }
    finally { second.close(); val paths = Files.walk(directory); try paths.sorted(java.util.Comparator.reverseOrder()).forEach(p => { Files.delete(p); () }) finally paths.close() }
  }
  test("poison quarantine is atomic, idempotent, and allows next event") {
    val db = sink(); val poison = Record("customers", 0, 0, None, Some("invalid-json"))
    try {
      intercept[IllegalStateException](db.process(poison, () => throw new IllegalStateException("crash")))
      assertEquals(db.snapshot()("quarantine").arr.size, 0)
      assertEquals(db.nextOffset("customers", 0), None)
      assertEquals(db.process(poison), Quarantined); assertEquals(db.process(poison), Duplicate)
      db.process(put(1, "gold")); assertEquals(db.snapshot()("quarantine").arr.size, 1)
      assertEquals(db.nextOffset("customers", 0), Some(2L))
    } finally db.close()
  }
  test("delete falls back to message key; tombstones advance checkpoint") {
    val db = sink()
    try {
      db.process(put(0, "gold"))
      assertEquals(db.process(Record("customers", 0, 1, Some("""{"payload":{"id":"a-0"}}"""), Some("""{"op":"d","before":null}"""))), Applied)
      db.process(Record("customers", 0, 2, None, None))
      assertEquals(db.snapshot()("projection").arr.size, 0); assertEquals(db.nextOffset("customers", 0), Some(3L))
    } finally db.close()
  }
  test("partitions have independent checkpoints, compacted offset gaps are allowed") {
    val db = sink()
    try { db.process(put(5, "a")); db.process(put(2, "b", 1)); assertEquals(db.nextOffset("customers", 0), Some(6L)); assertEquals(db.nextOffset("customers", 1), Some(3L)); assertEquals(db.snapshot()("projection").arr.size, 2) }
    finally db.close()
  }
  test("stale offset cannot overwrite a newer entity version") {
    val db = sink()
    try { db.process(put(4, "gold")); assertEquals(db.process(put(3, "bronze")), Duplicate); assertEquals(db.snapshot()("projection")(0)("body")("tier").str, "gold") }
    finally db.close()
  }
  test("unsupported operations and numeric IDs are quarantined with offset provenance") {
    val db = sink()
    try {
      assertEquals(db.process(Record("customers", 0, 0, None, Some("""{"op":"x"}"""))), Quarantined)
      assertEquals(db.process(Record("customers", 0, 1, None, Some("""{"op":"c","after":{"id":42}}"""))), Quarantined)
      assertEquals(db.snapshot()("quarantine")(1)("offset").str, "1")
    } finally db.close()
  }
  test("recovery refuses retention loss and log truncation instead of silently resetting") {
    intercept[IllegalArgumentException](RecoveryOffset.choose(Some(50), 100, 200))
    intercept[IllegalArgumentException](RecoveryOffset.choose(Some(250), 100, 200))
    assertEquals(RecoveryOffset.choose(Some(150), 100, 200), 150L)
    assertEquals(RecoveryOffset.choose(None, 100, 200), 100L)
  }
}
