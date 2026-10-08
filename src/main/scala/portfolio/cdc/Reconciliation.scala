package portfolio.cdc

import java.nio.file.{Files, Path}

final case class ProjectionRow(id: String, body: ujson.Value)
final case class ChangedRow(id: String, fields: Vector[String])
final case class ReconciliationReport(expectedRows: Int, actualRows: Int, missing: Vector[String], unexpected: Vector[String], changed: Vector[ChangedRow], sampleLimit: Int) {
  def consistent: Boolean = missing.isEmpty && unexpected.isEmpty && changed.isEmpty
  def json: ujson.Value = {
    def keys(values: Vector[String]): ujson.Value = ujson.Obj(
      "count" -> values.size,
      "keys" -> ujson.Arr.from(values.take(sampleLimit)),
      "truncated" -> (values.size > sampleLimit)
    )
    ujson.Obj(
      "consistent" -> consistent,
      "expectedRows" -> expectedRows,
      "actualRows" -> actualRows,
      "missing" -> keys(missing),
      "unexpected" -> keys(unexpected),
      "changed" -> ujson.Obj(
        "count" -> changed.size,
        "samples" -> ujson.Arr.from(changed.take(sampleLimit).map(row => ujson.Obj("id" -> row.id, "fields" -> ujson.Arr.from(row.fields)))),
        "truncated" -> (changed.size > sampleLimit)
      ),
      "scope" -> "Compare a fixed source export with a quiescent target; this report does not establish a shared live snapshot.",
      "sampleLimit" -> sampleLimit
    )
  }
}

object Reconciliation {
  val MaxRows = 10000
  val MaxExportBytes = 16L * 1024 * 1024
  private def identifier(row: ujson.Value): String = {
    require(row.objOpt.nonEmpty, "expected rows must be JSON objects")
    val id = row.obj.get("id").flatMap(_.strOpt).getOrElse(throw new IllegalArgumentException("expected rows need a string id"))
    require(id.nonEmpty && id.length <= 255, "invalid expected row id")
    id
  }
  private def validateBody(value: ujson.Value): Unit = value match {
    // ujson parses numbers as Double. Reject them rather than silently hiding precision loss.
    case _: ujson.Num => throw new IllegalArgumentException("numeric fields are not supported by exact reconciliation yet; use strings for precise values")
    case objectValue: ujson.Obj => objectValue.value.values.foreach(validateBody)
    case array: ujson.Arr => array.value.foreach(validateBody)
    case _ => ()
  }
  def readExpected(path: Path): Vector[ProjectionRow] = {
    // Limit the bytes read as well as the parsed row count; do not trust a separate file-size check.
    val input = Files.newInputStream(path)
    val bytes = try input.readNBytes((MaxExportBytes + 1).toInt) finally input.close()
    require(bytes.length <= MaxExportBytes, s"source export exceeds $MaxExportBytes bytes")
    val rows = ujson.read(new String(bytes, java.nio.charset.StandardCharsets.UTF_8)).arr.toVector
    require(rows.size <= MaxRows, s"source export exceeds $MaxRows rows")
    rows.map(row => ProjectionRow(identifier(row), row))
  }
  def compare(expected: Vector[ProjectionRow], actual: Vector[ProjectionRow], sampleLimit: Int = 20): ReconciliationReport = {
    require(sampleLimit >= 0 && sampleLimit <= 1000, "sample limit must be between 0 and 1000")
    require(expected.size <= MaxRows && actual.size <= MaxRows, s"reconciliation is limited to $MaxRows rows per side")
    require(expected.map(_.id).distinct.size == expected.size, "source export has duplicate primary keys")
    require(actual.map(_.id).distinct.size == actual.size, "target has duplicate primary keys")
    expected.foreach(row => { require(identifier(row.body) == row.id, "source key and body id differ"); validateBody(row.body) })
    actual.foreach(row => { require(row.body.objOpt.nonEmpty, "target rows must be JSON objects"); validateBody(row.body) })
    val expectedById = expected.map(row => row.id -> row.body).toMap
    val actualById = actual.map(row => row.id -> row.body).toMap
    val missing = (expectedById.keySet -- actualById.keySet).toVector.sorted
    val unexpected = (actualById.keySet -- expectedById.keySet).toVector.sorted
    val changed = (expectedById.keySet intersect actualById.keySet).toVector.sorted.flatMap { id =>
      val left = expectedById(id).obj; val right = actualById(id).obj
      val fields = (left.keySet ++ right.keySet).toVector.sorted.filter(field => left.get(field) != right.get(field))
      if (fields.isEmpty) None else Some(ChangedRow(id, fields))
    }
    ReconciliationReport(expected.size, actual.size, missing, unexpected, changed, sampleLimit)
  }
}
