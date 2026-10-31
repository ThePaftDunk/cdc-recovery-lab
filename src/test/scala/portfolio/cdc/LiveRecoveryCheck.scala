package portfolio.cdc

import java.net.{InetSocketAddress, ServerSocket, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Path, Paths}
import java.sql.{Connection, DriverManager}
import java.time.Duration
import java.util.{Collections, Properties, UUID}
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.{AdminClient, NewTopic, OffsetSpec, RecordsToDelete}
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Explicit opt-in integration runner; never connects to an externally supplied cluster/database. */
object LiveRecoveryCheck {
  def main(args: Array[String]): Unit = {
    require(args.isEmpty, "This runner owns its test stack and accepts no external target arguments")
    Class.forName("org.postgresql.Driver")
    val suffix = UUID.randomUUID().toString.take(12)
    val network = s"cdc-live-$suffix"
    val directory = Files.createTempDirectory("cdc-live-")
    val password = UUID.randomUUID().toString.replace("-", "")
    val containers = ArrayBuffer.empty[(String, String)]
    val phases = ArrayBuffer.empty[ujson.Value]
    val images = ujson.Obj()
    var networkCreated = false
    var worker: Option[Process] = None
    var workerLog: Path = directory.resolve("worker.log")
    var generation = 0
    var admin: Option[AdminClient] = None
    var producer: Option[KafkaProducer[String, String]] = None
    val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()
    val report = Paths.get("target/live-cdc-report.json")
    def scrub(value: String): String = value.replace(password, "[redacted]")
    def command(args: Seq[String], seconds: Long = 120): String = {
      val output = Files.createTempFile(directory, "command-", ".log")
      val process = new ProcessBuilder(args: _*).redirectErrorStream(true).redirectOutput(output.toFile).start()
      try {
        if (!process.waitFor(seconds, TimeUnit.SECONDS)) {
          process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS)
          throw new IllegalStateException(s"${args.head} timed out")
        }
        val text = Files.readString(output)
        require(process.exitValue() == 0, s"${args.head} failed: ${scrub(text).takeRight(4000)}")
        text.trim
      } finally Files.deleteIfExists(output)
    }
    def docker(args: String*): String = command("docker" +: args)
    def container(label: String, image: String, env: Map[String, String], ports: Seq[(Int, Int)], extra: Seq[String] = Nil): String = {
      val args = Seq("docker", "create", "--name", s"cdc-$label-$suffix", "--network", network, "--network-alias", label) ++
        ports.flatMap { case (host, inside) => Seq("--publish", s"127.0.0.1:$host:$inside") } ++
        env.toSeq.flatMap { case (key, value) => Seq("--env", s"$key=$value") } ++ Seq(image) ++ extra
      val id = command(args, 300).linesIterator.toVector.last.trim
      containers += label -> id
      docker("start", id)
      images(label) = docker("inspect", "--format", "{{.Image}}", id)
      id
    }
    def await(label: String, seconds: Long = 60)(check: => Boolean): Unit = {
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds)
      var last: Option[Throwable] = None
      var done = false
      while (!done && System.nanoTime() < deadline) {
        try { done = check } catch { case NonFatal(error) => last = Some(error) }
        if (!done) Thread.sleep(250)
      }
      require(done, s"Timed out waiting for $label" + last.map(e => s": ${scrub(e.toString)}").getOrElse(""))
    }
    def passed(label: String): Unit = { phases += ujson.Obj("check" -> label, "status" -> "passed"); println(s"PASS: $label") }
    def save(error: Option[String] = None): Unit = {
      Files.createDirectories(report.getParent)
      Files.writeString(report, ujson.Obj("scenario" -> "live PostgreSQL/Debezium/Kafka recovery", "javaVersion" -> System.getProperty("java.version"),
        "images" -> images, "checks" -> ujson.Arr.from(phases), "error" -> error.map(ujson.Str(_)).getOrElse(ujson.Null)).render(indent = 2))
    }
    val reserved = scala.collection.mutable.Set.empty[Int]
    def port(): Int = {
      var value = 0
      do { val socket = new ServerSocket(); socket.bind(new InetSocketAddress("127.0.0.1", 0)); value = socket.getLocalPort; socket.close() } while (reserved(value))
      reserved += value; value
    }
    val kafkaPort = port(); val sourcePort = port(); val sinkPort = port(); val connectPort = port(); val healthPort = port()
    val broker = s"127.0.0.1:$kafkaPort"
    val sourceUrl = s"jdbc:postgresql://127.0.0.1:$sourcePort/app?connectTimeout=2&socketTimeout=5"
    val sinkUrl = s"jdbc:postgresql://127.0.0.1:$sinkPort/sink?connectTimeout=2&socketTimeout=5"
    val tp = new TopicPartition("customers", 0)
    val group = s"recovery-$suffix"
    def jdbc[A](url: String)(action: Connection => A): A = {
      val db = DriverManager.getConnection(url, "demo", password)
      try action(db) finally db.close()
    }
    def sql(url: String, value: String): Unit = jdbc(url) { db => val statement = db.createStatement(); try { statement.execute(value); () } finally statement.close() }
    def ready(url: String): Boolean = jdbc(url)(_.isValid(2))
    def rows(url: String, source: Boolean): Map[String, String] = jdbc(url) { db =>
      val statement = db.createStatement()
      try {
        val result = statement.executeQuery(if (source) "SELECT id, tier FROM customers" else "SELECT entity_key, body FROM projection")
        val values = scala.collection.mutable.Map.empty[String, String]
        try while (result.next()) {
          val id = result.getString(1)
          val tier = if (source) result.getString(2) else {
            val body = ujson.read(result.getString(2))
            require(body.obj.keySet == Set("id", "tier") && body("id").str == id, "projection body/key mismatch")
            body("tier").str
          }
          values(id) = tier
        } finally result.close()
        values.toMap
      } finally statement.close()
    }
    def state(): ujson.Value = { val sink = new TransactionalSink(sinkUrl, "demo", password); try sink.snapshot() finally sink.close() }
    def checkpoint(): Long = { val sink = new TransactionalSink(sinkUrl, "demo", password); try sink.nextOffset("customers", 0).getOrElse(-1L) finally sink.close() }
    def end(): Long = admin.get.listOffsets(Collections.singletonMap(tp, OffsetSpec.latest())).all().get(5, TimeUnit.SECONDS).get(tp).offset()
    def committed(): Option[Long] = Option(admin.get.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS).get(tp)).map(_.offset())
    def request(path: String, method: String = "GET", body: String = ""): (Int, String) = {
      val builder = HttpRequest.newBuilder(URI.create(path)).timeout(Duration.ofSeconds(3))
      if (method == "POST") builder.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)) else builder.GET()
      val result = http.send(builder.build(), HttpResponse.BodyHandlers.ofString())
      result.statusCode() -> result.body()
    }
    def stopWorker(): Unit = worker.foreach { process =>
      if (process.isAlive) { process.destroy(); if (!process.waitFor(15, TimeUnit.SECONDS)) { process.destroyForcibly(); process.waitFor(5, TimeUnit.SECONDS) } }
      worker = None
    }
    def startWorker(): Process = {
      generation += 1; workerLog = directory.resolve(s"worker-$generation.log")
      val clientProperties = directory.resolve("worker-client.properties")
      Files.writeString(clientProperties, "session.timeout.ms=10000\nheartbeat.interval.ms=2000\n")
      val builder = new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java").toString, "-Xmx256m",
        "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "-jar", "target/scala-2.13/cdc-recovery-lab.jar", "kafka")
      val env = builder.environment()
      Seq("JDBC_URL" -> sinkUrl, "DB_USER" -> "demo", "DB_PASSWORD" -> password, "KAFKA_BOOTSTRAP_SERVERS" -> broker,
        "KAFKA_GROUP_ID" -> group, "KAFKA_TOPIC" -> "customers", "HEALTH_PORT" -> healthPort.toString).foreach { case (key, value) => env.put(key, value) }
      env.put("KAFKA_CLIENT_PROPERTIES", clientProperties.toString)
      val process = builder.redirectErrorStream(true).redirectOutput(workerLog.toFile).start()
      worker = Some(process); process
    }
    def metrics(): String = request(s"http://127.0.0.1:$healthPort/metrics")._2
    def outcomeCount(label: String, count: Long): Boolean = metrics().linesIterator.contains(s"cdc_records_total{outcome=\"$label\"} $count")
    def caughtUp(): Boolean = checkpoint() == end() && committed().contains(end())
    def publish(value: String): Long = producer.get.send(new ProducerRecord[String, String]("customers", 0, "synthetic", value)).get(5, TimeUnit.SECONDS).offset()
    try {
      docker("info", "--format", "{{.ServerVersion}}")
      docker("network", "create", network); networkCreated = true
      val pgEnv = Map("POSTGRES_USER" -> "demo", "POSTGRES_PASSWORD" -> password)
      val postgresImage = "postgres:16.6@sha256:557fea37a744d5f4c8faab304b0a90858b53ab119735a88c131fd19dab802f36"
      container("source-db", postgresImage, pgEnv + ("POSTGRES_DB" -> "app"), Seq(sourcePort -> 5432),
        Seq("postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=4", "-c", "max_wal_senders=4"))
      container("sink-db", postgresImage, pgEnv + ("POSTGRES_DB" -> "sink"), Seq(sinkPort -> 5432))
      container("kafka", "apache/kafka:4.0.2@sha256:836cafdad9f4825880d7cf1d5a21202915ae2527bd0ef1c3600c526ed7814d1f", Map(
        "KAFKA_NODE_ID" -> "1", "KAFKA_PROCESS_ROLES" -> "broker,controller", "KAFKA_LISTENERS" -> "CLIENT://:9092,INTERNAL://:29092,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS" -> s"CLIENT://$broker,INTERNAL://kafka:29092", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP" -> "CLIENT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "KAFKA_INTER_BROKER_LISTENER_NAME" -> "INTERNAL", "KAFKA_CONTROLLER_LISTENER_NAMES" -> "CONTROLLER", "KAFKA_CONTROLLER_QUORUM_VOTERS" -> "1@kafka:9093",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR" -> "1", "KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS" -> "1", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS" -> "0",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR" -> "1", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR" -> "1", "KAFKA_NUM_PARTITIONS" -> "1",
        "KAFKA_LOG_DIRS" -> "/tmp/kafka-data", "KAFKA_HEAP_OPTS" -> "-Xms256m -Xmx512m", "CLUSTER_ID" -> "MkU3OEVBNTcwNTJENDM2Qk"), Seq(kafkaPort -> 9092))
      await("source PostgreSQL")(ready(sourceUrl)); await("sink PostgreSQL")(ready(sinkUrl))
      sql(sourceUrl, Files.readString(Paths.get("ops/source.sql")))
      val props = new Properties(); props.setProperty("bootstrap.servers", broker); props.setProperty("default.api.timeout.ms", "10000"); props.setProperty("request.timeout.ms", "5000")
      val kafka = AdminClient.create(props); admin = Some(kafka)
      await("Kafka metadata")(kafka.listTopics().names().get(5, TimeUnit.SECONDS) != null)
      kafka.createTopics(Collections.singletonList(new NewTopic("customers", 1, 1.toShort))).all().get(10, TimeUnit.SECONDS)
      container("connect", "quay.io/debezium/connect:3.0@sha256:5547ddf063d6c20c56b56d660eb6bc449b180e6a70b2d69fc981629672b1f213", Map("BOOTSTRAP_SERVERS" -> "kafka:29092", "GROUP_ID" -> "recovery-connect",
        "CONFIG_STORAGE_TOPIC" -> "connect-configs", "OFFSET_STORAGE_TOPIC" -> "connect-offsets", "STATUS_STORAGE_TOPIC" -> "connect-status",
        "CONFIG_STORAGE_REPLICATION_FACTOR" -> "1", "OFFSET_STORAGE_REPLICATION_FACTOR" -> "1", "STATUS_STORAGE_REPLICATION_FACTOR" -> "1",
        "KAFKA_HEAP_OPTS" -> "-Xms256m -Xmx512m"), Seq(connectPort -> 8083))
      val rest = s"http://127.0.0.1:$connectPort"
      await("Kafka Connect REST", 120)(request(s"$rest/connectors")._1 == 200)
      val config = ujson.read(Files.readString(Paths.get("ops/connector.json")))
      config("database.password") = password
      val registration = request(s"$rest/connectors", "POST", ujson.Obj("name" -> "customers-cdc", "config" -> config).render())
      require(registration._1 == 201, s"connector registration failed: ${scrub(registration._2)}")
      await("Debezium initial snapshot", 120)(end() == 2L)
      val status = ujson.read(request(s"$rest/connectors/customers-cdc/status")._2)
      require(status("connector")("state").str == "RUNNING" && status("tasks").arr.forall(_("state").str == "RUNNING"))
      passed("real Debezium pgoutput snapshot reaches Kafka")

      val consumerProps = new Properties(); consumerProps.putAll(props)
      consumerProps.setProperty("group.id", group); consumerProps.setProperty("enable.auto.commit", "false")
      consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      val consumer = new KafkaConsumer[String, String](consumerProps)
      val first = try {
        consumer.assign(Collections.singletonList(tp)); consumer.seek(tp, 0L)
        var records = Vector.empty[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]]
        await("snapshot record")( { records = consumer.poll(Duration.ofSeconds(1)).asScala.toVector; records.nonEmpty } )
        val raw = records.head
        Record(raw.topic(), raw.partition(), raw.offset(), Option(raw.key()), Option(raw.value()))
      } finally consumer.close(Duration.ofSeconds(5)) // Deliberately do not acknowledge.
      val sink = new TransactionalSink(sinkUrl, "demo", password)
      try {
        var injected = false
        try sink.process(first, () => throw new IllegalStateException("injected pre-commit failure")) catch { case _: IllegalStateException => injected = true }
        require(injected && sink.nextOffset("customers", 0).isEmpty && sink.snapshot()("projection").arr.isEmpty, "PostgreSQL rollback failed")
        passed("PostgreSQL projection and checkpoint roll back together")
        require(sink.process(first) == Applied && sink.nextOffset("customers", 0).contains(1L))
      } finally sink.close()
      require(committed().isEmpty, "first consumer unexpectedly acknowledged its record")
      passed("PostgreSQL commit survives close without a Kafka acknowledgement")
      val running = startWorker()
      await("actual worker snapshot recovery")(running.isAlive && rows(sinkUrl, false) == rows(sourceUrl, true) && caughtUp())
      require(outcomeCount("applied", 1) && outcomeCount("duplicate", 0), "worker did not seek the durable checkpoint")
      passed("packaged Scala worker resumes at the PostgreSQL checkpoint")

      sql(sourceUrl, "INSERT INTO customers VALUES ('customer-c', 'bronze'); UPDATE customers SET tier='gold' WHERE id='customer-a'; DELETE FROM customers WHERE id='customer-b'")
      await("WAL insert/update/delete convergence")(rows(sinkUrl, false) == Map("customer-a" -> "gold", "customer-c" -> "bronze") && caughtUp())
      require(rows(sinkUrl, false) == rows(sourceUrl, true), "independent source/target SQL comparison failed")
      passed("real WAL insert/update/delete converges with the source table")
      val producerProps = new Properties(); producerProps.putAll(props)
      producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      producer = Some(new KafkaProducer[String, String](producerProps))
      val poisonOffset = publish("invalid-json")
      await("atomic poison quarantine")(state()("quarantine").arr.size == 1 && caughtUp())
      require(state()("quarantine")(0)("offset").str == poisonOffset.toString)
      sql(sourceUrl, "UPDATE customers SET tier='platinum' WHERE id='customer-c'")
      await("progress after poison")(rows(sinkUrl, false) == rows(sourceUrl, true) && rows(sinkUrl, false)("customer-c") == "platinum" && caughtUp())
      require(state()("quarantine").arr.size == 1)
      passed("poison quarantine is durable and later WAL changes still progress")

      stopWorker()
      val beforeRestart = checkpoint()
      await("stopped worker group to become empty", 40) {
        kafka.describeConsumerGroups(Collections.singletonList(group)).all().get(5, TimeUnit.SECONDS).get(group).state().toString.equalsIgnoreCase("empty")
      }
      kafka.alterConsumerGroupOffsets(group, Collections.singletonMap(tp, new OffsetAndMetadata(0L))).all().get(10, TimeUnit.SECONDS)
      require(committed().contains(0L))
      startWorker()
      await("restarted worker assignment")(request(s"http://127.0.0.1:$healthPort/health/ready")._1 == 200)
      sql(sourceUrl, "UPDATE customers SET tier='diamond' WHERE id='customer-a'")
      await("recovery from stale Kafka acknowledgement")(checkpoint() > beforeRestart && rows(sinkUrl, false) == rows(sourceUrl, true) && rows(sinkUrl, false)("customer-a") == "diamond" && caughtUp())
      require(outcomeCount("applied", 1) && outcomeCount("duplicate", 0) && outcomeCount("quarantined", 0))
      passed("actual worker restart ignores a deliberately stale group offset")

      stopWorker()
      val durable = checkpoint(); val beforeRetention = state()
      require(publish("""{"op":"u","after":{"id":"customer-a","tier":"must-not-apply"}}""") == durable)
      kafka.deleteRecords(Collections.singletonMap(tp, RecordsToDelete.beforeOffset(durable + 1))).all().get(10, TimeUnit.SECONDS)
      val refused = startWorker()
      require(refused.waitFor(30, TimeUnit.SECONDS) && refused.exitValue() != 0, "worker silently reset after retention loss")
      require(Files.readString(workerLog).contains("durable checkpoint outside retained log"), "worker failed for a different reason")
      require(state() == beforeRetention && committed().contains(durable), "retention refusal changed durable state or acknowledged")
      passed("real retained-log loss fails closed without changing target or offsets")
      save(); println(s"PASS: ${phases.size} live CDC checks; report: $report")
    } catch {
      case NonFatal(error) =>
        save(Some(scrub(error.toString)))
        if (Files.exists(workerLog)) System.err.println(scrub(Files.readString(workerLog)).takeRight(6000))
        containers.foreach { case (label, id) => try System.err.println(s"$label: " + scrub(docker("logs", "--tail", "15", id)).takeRight(4000)) catch { case NonFatal(_) => () } }
        throw error
    } finally {
      stopWorker(); producer.foreach(_.close(Duration.ofSeconds(5))); admin.foreach(_.close(Duration.ofSeconds(5)))
      containers.reverseIterator.foreach { case (_, id) => docker("rm", "--force", "--volumes", id) }
      if (networkCreated) docker("network", "rm", network)
      http.close()
      val paths = Files.walk(directory)
      try paths.sorted(java.util.Comparator.reverseOrder()).forEach(path => { Files.deleteIfExists(path); () }) finally paths.close()
    }
  }
}
