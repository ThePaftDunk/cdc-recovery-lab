package portfolio.cdc

import java.nio.file.{Files, Paths}
import java.time.Duration
import java.util.{Collections, Properties}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{FileIO, Framing, Sink, Source}
import org.apache.pekko.util.ByteString
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object Main {
  def main(args: Array[String]): Unit = args.headOption.getOrElse("demo") match {
    case "demo" => demo()
    case "file" =>
      require(args.length == 2, "usage: run file path/to/records.jsonl")
      val sink = configuredSink()
      try {
        val outcomes = streamFile(args(1), sink)
        println(ujson.Obj("outcomes" -> ujson.Obj.from(outcomes.map { case (label, count) => label -> ujson.Str(count.toString) })).render(indent = 2))
      } finally sink.close()
    case "inspect" => val sink = configuredSink(); try println(sink.snapshot().render(indent = 2)) finally sink.close()
    case "reconcile" =>
      require(args.length == 2, "usage: run reconcile path/to/source-export.json")
      val expected = Reconciliation.readExpected(Paths.get(args(1)))
      val actual = ProjectionReader.read(sys.env.getOrElse("JDBC_URL", "jdbc:h2:file:./.data/sink;MODE=PostgreSQL"), sys.env.getOrElse("DB_USER", "sa"), sys.env.getOrElse("DB_PASSWORD", ""))
      val report = Reconciliation.compare(expected, actual)
      println(report.json.render(indent = 2))
      if (!report.consistent) sys.exit(2)
    case "kafka" => kafka()
    case other => throw new IllegalArgumentException(s"unknown mode: $other")
  }
  private def configuredSink(): TransactionalSink = {
    Files.createDirectories(Paths.get(".data"))
    new TransactionalSink(sys.env.getOrElse("JDBC_URL", "jdbc:h2:file:./.data/sink;MODE=PostgreSQL"), sys.env.getOrElse("DB_USER", "sa"), sys.env.getOrElse("DB_PASSWORD", ""))
  }
  private def readRecords(path: String): Vector[Record] = {
    val lines = scala.io.Source.fromFile(path)
    try lines.getLines().filter(_.trim.nonEmpty).map { line =>
      parseRecord(line)
    }.toVector finally lines.close()
  }
  private def parseRecord(line: String): Record = {
    val json = ujson.read(line)
    def optional(name: String): Option[String] = json.obj.get(name).filter(_ != ujson.Null).map(_.render())
    val partition = json("partition").num
    require(partition.isWhole && partition >= 0 && partition <= Int.MaxValue, "partition must be a non-negative integer")
    Record(json("topic").str, partition.toInt, json("offset").str.toLong, optional("key"), optional("value"))
  }
  private def streamFile(path: String, sink: TransactionalSink): Map[String, Long] = {
    implicit val system: ActorSystem = ActorSystem("file-recovery-lab")
    implicit val executionContext: scala.concurrent.ExecutionContext = system.dispatchers.lookup("pekko.actor.default-blocking-io-dispatcher")
    try Await.result(FileIO.fromPath(Paths.get(path))
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024 * 1024, allowTruncation = true))
      .map(_.utf8String).filter(_.trim.nonEmpty).map(parseRecord)
      .mapAsync(1)(r => Future(sink.process(r)))
      .runFold(Map.empty[String, Long])((counts, outcome) => counts.updated(outcome.label, counts.getOrElse(outcome.label, 0L) + 1L)), scala.concurrent.duration.Duration.Inf)
    finally Await.result(system.terminate(), 10.seconds)
  }
  private def stream(records: Vector[Record], sink: TransactionalSink): Seq[Outcome] = {
    implicit val system: ActorSystem = ActorSystem("recovery-lab")
    implicit val executionContext: scala.concurrent.ExecutionContext = system.dispatchers.lookup("pekko.actor.default-blocking-io-dispatcher")
    try Await.result(Source(records).mapAsync(1)(r => Future(sink.process(r))).runWith(Sink.seq), 30.seconds)
    finally Await.result(system.terminate(), 10.seconds)
  }
  private def demo(): Unit = {
    Files.createDirectories(Paths.get(".data"))
    // A unique persistent database proves close/reopen; demos do not overwrite earlier runs.
    val directory = Files.createTempDirectory(Paths.get(".data"), "demo-")
    val url = "jdbc:h2:file:" + directory.resolve("sink").toAbsolutePath + ";MODE=PostgreSQL"
    val records = readRecords("fixtures/changes.jsonl")
    val first = new TransactionalSink(url, "sa")
    try {
      first.process(records.head)
      try first.process(records(1), () => throw new IllegalStateException("simulated crash before commit"))
      catch { case _: IllegalStateException => println("1. Crash before commit: projection and checkpoint rolled back.") }
      require(first.nextOffset("customers", 0).contains(1L), "rollback did not preserve checkpoint")
      first.process(records(1))
      println("2. Sink committed offset 1; simulate loss of source acknowledgement by closing.")
    } finally first.close()
    val restarted = new TransactionalSink(url, "sa")
    try {
      val outcomes = stream(records, restarted)
      require(outcomes.count(_ == Duplicate) == 2)
      require(outcomes.count(_ == Quarantined) == 1)
      require(restarted.nextOffset("customers", 0).contains(6L))
      val state = restarted.snapshot()
      require(state("projection").arr.size == 1 && state("projection")(0)("body")("tier").str == "gold")
      val report = ujson.Obj("scenario" -> "rollback + commit-before-ack + restart", "outcomes" -> ujson.Arr.from(outcomes.map(_.label)), "state" -> state, "database" -> directory.toString)
      val reportPath = directory.resolve("report.json")
      Files.writeString(reportPath, report.render(indent = 2))
      println("3. Replayed committed offsets were skipped; poison record quarantined; delete/tombstone applied.")
      println(report.render(indent = 2))
      println(s"Recovery report saved to $reportPath")
    } finally restarted.close()
  }
  private def kafka(): Unit = {
    val sink = configuredSink()
    val running = new AtomicBoolean(true)
    val assigned = new AtomicBoolean(false)
    val applied = new AtomicLong(); val duplicates = new AtomicLong(); val quarantined = new AtomicLong()
    val topic = sys.env.getOrElse("KAFKA_TOPIC", "customers")
    val properties = new Properties()
    sys.env.get("KAFKA_CLIENT_PROPERTIES").foreach { file =>
      val input = Files.newInputStream(Paths.get(file)); try properties.load(input) finally input.close()
    }
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, sys.env.getOrElse("KAFKA_GROUP_ID", "cdc-recovery-lab"))
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")
    val consumer = new KafkaConsumer[String, String](properties)
    val health = HttpServer.create(new InetSocketAddress(sys.env.getOrElse("HEALTH_PORT", "8081").toInt), 8)
    health.createContext("/health/live", x => { val bytes = "alive".getBytes("UTF-8"); x.sendResponseHeaders(200, bytes.length); x.getResponseBody.write(bytes); x.close() })
    health.createContext("/health/ready", x => { val ok = running.get() && assigned.get() && sink.healthy; val bytes = (if (ok) "ready" else "unavailable").getBytes("UTF-8"); x.sendResponseHeaders(if (ok) 200 else 503, bytes.length); x.getResponseBody.write(bytes); x.close() })
    health.createContext("/metrics", x => {
      val bytes = s"cdc_records_total{outcome=\"applied\"} ${applied.get()}\ncdc_records_total{outcome=\"duplicate\"} ${duplicates.get()}\ncdc_records_total{outcome=\"quarantined\"} ${quarantined.get()}\n".getBytes("UTF-8")
      x.sendResponseHeaders(200, bytes.length); x.getResponseBody.write(bytes); x.close()
    })
    Runtime.getRuntime.addShutdownHook(new Thread(() => { running.set(false); consumer.wakeup() }))
    health.start()
    try {
      consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener {
        override def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]): Unit = assigned.set(false)
        override def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]): Unit = {
          val starts = consumer.beginningOffsets(partitions)
          val ends = consumer.endOffsets(partitions)
          partitions.asScala.foreach { partition =>
            val low = starts.get(partition).longValue(); val high = ends.get(partition).longValue()
            val offset = RecoveryOffset.choose(sink.nextOffset(partition.topic(), partition.partition()), low, high)
            consumer.seek(partition, offset)
          }
          assigned.set(!partitions.isEmpty)
        }
      })
      while (running.get()) {
        consumer.poll(Duration.ofMillis(500)).asScala.foreach { raw =>
          sink.process(Record(raw.topic(), raw.partition(), raw.offset(), Option(raw.key()), Option(raw.value()))) match {
            case Applied => applied.incrementAndGet()
            case Duplicate => duplicates.incrementAndGet()
            case Quarantined => quarantined.incrementAndGet()
          }
          // Commit this partition's next offset only after its JDBC transaction succeeds.
          consumer.commitSync(Collections.singletonMap(new TopicPartition(raw.topic(), raw.partition()), new OffsetAndMetadata(raw.offset() + 1)))
        }
      }
    } catch { case _: WakeupException if !running.get() => () }
    finally { running.set(false); health.stop(1); consumer.close(); sink.close() }
  }
}
