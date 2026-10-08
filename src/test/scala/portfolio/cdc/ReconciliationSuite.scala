package portfolio.cdc

import java.nio.file.Files

class ReconciliationSuite extends munit.FunSuite {
  private def row(id: String, tier: String): ProjectionRow = ProjectionRow(id, ujson.Obj("id" -> id, "tier" -> tier))
  test("missing, unexpected and changed rows are reported with deterministic keys") {
    val report = Reconciliation.compare(Vector(row("a", "gold"), row("missing", "silver")), Vector(row("a", "bronze"), row("extra", "gold")))
    assert(!report.consistent)
    assertEquals(report.missing, Vector("missing")); assertEquals(report.unexpected, Vector("extra"))
    assertEquals(report.changed, Vector(ChangedRow("a", Vector("tier"))))
    assert(!report.json.render().contains("bronze"), "report must not expose row payloads")
  }
  test("object order is ignored, array order and missing-vs-null are significant") {
    val expected = ProjectionRow("a", ujson.read("""{"id":"a","prefs":{"enabled":true,"tags":["x","y"]},"optional":null}"""))
    val reordered = ProjectionRow("a", ujson.read("""{"optional":null,"prefs":{"tags":["x","y"],"enabled":true},"id":"a"}"""))
    assert(Reconciliation.compare(Vector(expected), Vector(reordered)).consistent)
    val different = ProjectionRow("a", ujson.read("""{"id":"a","prefs":{"enabled":true,"tags":["y","x"]}}"""))
    assertEquals(Reconciliation.compare(Vector(expected), Vector(different)).changed.head.fields, Vector("optional", "prefs"))
  }
  test("samples can be truncated without hiding the full difference count") {
    val report = Reconciliation.compare(Vector(row("c", "gold"), row("b", "gold"), row("a", "gold")), Vector.empty, sampleLimit = 1)
    assertEquals(report.json("missing")("count").num.toInt, 3)
    assertEquals(report.json("missing")("keys")(0).str, "a")
    assert(report.json("missing")("truncated").bool)
    assert(!Reconciliation.compare(Vector(row("a", "gold")), Vector.empty, 0).consistent)
  }
  test("duplicate exports, numeric values and invalid identities fail instead of implying consistency") {
    intercept[IllegalArgumentException](Reconciliation.compare(Vector(row("a", "gold"), row("a", "gold")), Vector.empty))
    val numeric = ProjectionRow("a", ujson.read("""{"id":"a","amount":9007199254740993}"""))
    intercept[IllegalArgumentException](Reconciliation.compare(Vector(numeric), Vector(numeric)))
    intercept[IllegalArgumentException](Reconciliation.compare(Vector(ProjectionRow("a", ujson.Obj("id" -> "b"))), Vector.empty))
    val corrupt = ProjectionRow("a", ujson.Obj("id" -> "wrong", "tier" -> "gold"))
    assertEquals(Reconciliation.compare(Vector(row("a", "gold")), Vector(corrupt)).changed.head.fields, Vector("id"))
  }
  test("JDBC reconciliation reads durable contents without changing checkpoints or quarantined records") {
    val url = "jdbc:h2:mem:" + java.util.UUID.randomUUID() + ";MODE=PostgreSQL"
    val sink = new TransactionalSink(url, "sa")
    try {
      sink.process(Record("customers", 0, 0, None, Some("""{"op":"r","after":{"id":"a","tier":"gold"}}""")))
      sink.process(Record("customers", 0, 1, None, Some("poison")))
      val before = sink.snapshot()
      val report = Reconciliation.compare(Vector(row("a", "gold")), ProjectionReader.read(url, "sa", ""))
      assert(report.consistent)
      assertEquals(sink.snapshot(), before)
    } finally sink.close()
  }
  test("source row limits reject oversized comparisons; an absent database is not created") {
    intercept[IllegalArgumentException](Reconciliation.compare(Vector.fill(Reconciliation.MaxRows + 1)(row("a", "gold")), Vector.empty))
    val directory = Files.createTempDirectory("reconcile-missing")
    try {
      intercept[java.sql.SQLException](ProjectionReader.read("jdbc:h2:file:" + directory.resolve("absent"), "sa", ""))
      val entries = Files.list(directory); try assertEquals(entries.count(), 0L) finally entries.close()
    } finally Files.delete(directory)
  }
  test("source export file round-trips and rejects malformed shape") {
    val path = Files.createTempFile("reconciliation-export", ".json")
    try {
      Files.writeString(path, """[{"id":"a","tier":"gold"}]""")
      assert(Reconciliation.compare(Reconciliation.readExpected(path), Vector(row("a", "gold"))).consistent)
      Files.writeString(path, """[{"id":42}]""")
      intercept[IllegalArgumentException](Reconciliation.readExpected(path))
    } finally Files.delete(path)
  }
}
