ThisBuild / scalaVersion := "2.13.16"
ThisBuild / organization := "portfolio"
ThisBuild / version := "1.0.0"
Compile / mainClass := Some("portfolio.cdc.Main")
Compile / run / fork := true
libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-stream" % "1.1.3",
  "com.lihaoyi" %% "ujson" % "4.1.0",
  "com.h2database" % "h2" % "2.3.232",
  "org.postgresql" % "postgresql" % "42.7.5",
  "org.apache.kafka" % "kafka-clients" % "4.0.0",
  "org.slf4j" % "slf4j-simple" % "2.0.17",
  "org.scalameta" %% "munit" % "1.0.2" % Test
)
scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlint")
Test / parallelExecution := false
addCommandAlias("demo", "run demo")
assembly / assemblyJarName := "cdc-recovery-lab.jar"
assembly / assemblyMergeStrategy := {
  case path if path.endsWith("module-info.class") => MergeStrategy.discard
  case path if path.startsWith("META-INF/") && (path.endsWith(".SF") || path.endsWith(".RSA") || path.endsWith(".DSA")) => MergeStrategy.discard
  case path => (assembly / assemblyMergeStrategy).value(path)
}
