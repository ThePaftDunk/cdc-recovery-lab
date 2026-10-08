package portfolio.cdc

import java.sql.{Connection, DriverManager}
import scala.util.control.NonFatal

sealed trait Outcome { def label: String }
case object Applied extends Outcome { val label = "applied" }
case object Duplicate extends Outcome { val label = "duplicate" }
case object Quarantined extends Outcome { val label = "quarantined" }

/** All projection changes, quarantine entries and checkpoints share one JDBC transaction. */
final class TransactionalSink(url: String, user: String = "", password: String = "") extends AutoCloseable {
  if (url.startsWith("jdbc:h2:")) Class.forName("org.h2.Driver")
  else if (url.startsWith("jdbc:postgresql:")) Class.forName("org.postgresql.Driver")
  private val db: Connection = DriverManager.getConnection(url, user, password)
  db.setAutoCommit(false)
  private val ddl = Seq(
    "CREATE TABLE IF NOT EXISTS projection (entity_key VARCHAR(255) PRIMARY KEY, body TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS checkpoints (source_topic VARCHAR(255), source_partition INT, next_offset BIGINT NOT NULL, PRIMARY KEY(source_topic, source_partition))",
    "CREATE TABLE IF NOT EXISTS quarantine (source_topic VARCHAR(255), source_partition INT, record_offset BIGINT, raw_body TEXT, reason TEXT NOT NULL, PRIMARY KEY(source_topic, source_partition, record_offset))"
  )
  ddl.foreach { sql => val statement = db.createStatement(); try statement.execute(sql) finally statement.close() }
  db.commit()

  private def execute(sql: String, values: Any*): Unit = {
    val statement = db.prepareStatement(sql)
    try { values.zipWithIndex.foreach { case (value, index) => statement.setObject(index + 1, value) }; statement.executeUpdate(); () }
    finally statement.close()
  }
  private def checkpoint(topic: String, partition: Int, lock: Boolean): Option[Long] = {
    val query = db.prepareStatement("SELECT next_offset FROM checkpoints WHERE source_topic=? AND source_partition=?" + (if (lock) " FOR UPDATE" else ""))
    try {
      query.setString(1, topic); query.setInt(2, partition)
      val rows = query.executeQuery(); try if (rows.next()) Some(rows.getLong(1)) else None finally rows.close()
    } finally query.close()
  }
  def nextOffset(topic: String, partition: Int): Option[Long] = synchronized {
    try checkpoint(topic, partition, lock = false) finally db.rollback()
  }
  def process(record: Record, beforeCommit: () => Unit = () => ()): Outcome = synchronized {
    try {
      val next = checkpoint(record.topic, record.partition, lock = true).getOrElse {
        // Kafka owns each partition. A concurrent first owner race fails safely and is retried after restart.
        execute("INSERT INTO checkpoints VALUES (?, ?, ?)", record.topic, record.partition, 0L)
        0L
      }
      if (record.offset < next) { db.rollback(); Duplicate }
      else {
        val decoded = try Right(Debezium.decode(record)) catch { case NonFatal(error) => Left(error.getMessage) }
        val outcome = decoded match {
          case Right(Put(id, body)) =>
            execute("DELETE FROM projection WHERE entity_key=?", id)
            execute("INSERT INTO projection VALUES (?, ?)", id, body)
            Applied
          case Right(Remove(id)) => execute("DELETE FROM projection WHERE entity_key=?", id); Applied
          case Right(Tombstone) => Applied
          case Left(reason) =>
            execute("INSERT INTO quarantine VALUES (?, ?, ?, ?, ?)", record.topic, record.partition, record.offset, record.value.orNull, Option(reason).getOrElse("decode failure").take(2000))
            Quarantined
        }
        execute("UPDATE checkpoints SET next_offset=? WHERE source_topic=? AND source_partition=?", record.offset + 1, record.topic, record.partition)
        beforeCommit() // Fault injection: a throw here must roll back every write.
        db.commit()
        outcome
      }
    } catch { case NonFatal(error) => db.rollback(); throw error }
  }
  def snapshot(): ujson.Value = synchronized {
    def rows(sql: String)(read: java.sql.ResultSet => ujson.Value): ujson.Arr = {
      val statement = db.createStatement()
      try { val result = statement.executeQuery(sql); val values = ujson.Arr(); try while (result.next()) values.value += read(result) finally result.close(); values }
      finally statement.close()
    }
    try ujson.Obj(
      "projection" -> rows("SELECT entity_key, body FROM projection ORDER BY entity_key")(r => ujson.Obj("id" -> r.getString(1), "body" -> ujson.read(r.getString(2)))),
      "checkpoints" -> rows("SELECT source_topic, source_partition, next_offset FROM checkpoints ORDER BY source_topic, source_partition")(r => ujson.Obj("topic" -> r.getString(1), "partition" -> r.getInt(2), "nextOffset" -> r.getLong(3).toString)),
      "quarantine" -> rows("SELECT source_topic, source_partition, record_offset, reason FROM quarantine ORDER BY source_topic, source_partition, record_offset")(r => ujson.Obj("topic" -> r.getString(1), "partition" -> r.getInt(2), "offset" -> r.getLong(3).toString, "reason" -> r.getString(4)))
    ) finally db.rollback()
  }
  def healthy: Boolean = synchronized { db.isValid(2) }
  override def close(): Unit = synchronized { db.close() }
}
