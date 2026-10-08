package portfolio.cdc

final case class Record(topic: String, partition: Int, offset: Long, key: Option[String], value: Option[String]) {
  require(topic.nonEmpty && topic.length <= 255 && partition >= 0 && offset >= 0 && offset < Long.MaxValue)
}
sealed trait Change
final case class Put(id: String, body: String) extends Change
final case class Remove(id: String) extends Change
case object Tombstone extends Change

object RecoveryOffset {
  def choose(checkpoint: Option[Long], low: Long, high: Long): Long = {
    require(low >= 0 && high >= low, "invalid broker bounds")
    val next = checkpoint.getOrElse(low)
    require(next >= low && next <= high, "durable checkpoint outside retained log; restore from a verified snapshot")
    next
  }
}

object Debezium {
  // Explicitly constrained to a single table with non-empty string primary keys.
  def decode(record: Record): Change = record.value match {
    case None => Tombstone
    case Some(raw) =>
      val root = ujson.read(raw)
      val payload = root.objOpt.flatMap(_.get("payload")).getOrElse(root)
      if (payload == ujson.Null) Tombstone
      else {
        def identifier(value: ujson.Value): String = {
          val id = value("id").str
          require(id.nonEmpty && id.length <= 255, "id must be a non-empty string, max 255 characters")
          id
        }
        payload("op").str match {
          case "r" | "c" | "u" => val after = payload("after"); Put(identifier(after), after.render())
          case "d" =>
            val before = payload.obj.getOrElse("before", ujson.Null)
            val id = if (before != ujson.Null) identifier(before) else {
              val key = ujson.read(record.key.getOrElse(throw new IllegalArgumentException("delete has no before or key")))
              identifier(key.objOpt.flatMap(_.get("payload")).getOrElse(key))
            }
            Remove(id)
          case operation => throw new IllegalArgumentException(s"unsupported operation: $operation")
        }
      }
  }
}
