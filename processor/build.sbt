// Versions pinned per ADR-0001 (verified against Maven Central 2026-10-21).
val sparkVersion          = "4.1.3"
val postgresJdbcVersion   = "42.7.14"
val jacksonVersion        = "2.21.2"   // must equal Spark 4.1.3's fasterxml.jackson.version (AC-3.7)
val scalatestVersion      = "3.2.20"
val scalatestPlusVersion  = "3.2.20.0" // scalatestplus scalacheck-1-19
val jsonSchemaValidatorVersion = "1.5.9" // test only: validates DLQ output against contracts/
val testcontainersVersion = "2.0.5"

ThisBuild / organization := "ptp"
ThisBuild / version      := "0.1.0-SNAPSHOT"
// Must equal the Scala version Spark 4.1.3 is built with (spark-parent_2.13 POM).
ThisBuild / scalaVersion := "2.13.17"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlint", "-release", "17")

// JVM flags Spark needs on Java 17 (mirrors Spark's JavaModuleOptions; spark-submit adds them itself).
val sparkJava17Options = Seq(
  "-XX:+IgnoreUnrecognizedVMOptions",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false",
  "-Dio.netty.tryReflectionSetAccessible=true"
)

val testDeps = Seq(
  "org.scalatest"     %% "scalatest"       % scalatestVersion     % Test,
  "org.scalatestplus" %% "scalacheck-1-19" % scalatestPlusVersion % Test
)

// Repository root, so tests can read contracts/ and generator fixtures in place.
val repoRoot = settingKey[File]("Repository root")
ThisBuild / repoRoot := (ThisBuild / baseDirectory).value.getParentFile

// Pure domain logic: validation, ordering, status rules. No Spark/Kafka/JDBC (spec 03).
lazy val core = (project in file("core"))
  .settings(
    name := "ptp-processor-core",
    libraryDependencies ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.networknt" % "json-schema-validator" % jsonSchemaValidatorVersion % Test
    ) ++ testDeps,
    Test / fork := true,
    Test / javaOptions += s"-Dptp.repoRoot=${(ThisBuild / repoRoot).value.getAbsolutePath}"
  )

// Spark Structured Streaming jobs. Spark is Provided: the apache/spark runtime image supplies it.
lazy val streaming = (project in file("streaming"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    name := "ptp-processor-streaming",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"            % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % Provided,
      "org.postgresql"    % "postgresql"           % postgresJdbcVersion,
      "org.testcontainers" % "testcontainers-kafka"      % testcontainersVersion % Test,
      "org.testcontainers" % "testcontainers-postgresql" % testcontainersVersion % Test
    ) ++ testDeps,
    Test / fork := true,
    Test / javaOptions ++= sparkJava17Options ++ Seq("-Xmx2g", s"-Dptp.repoRoot=${(ThisBuild / repoRoot).value.getAbsolutePath}"),
    Test / parallelExecution := false,
    Test / envVars ++= sys.env.get("DOCKER_HOST").map("DOCKER_HOST" -> _).toMap,
    // `make process`: run with Spark (Provided) on the classpath, forked with the Java 17 flags.
    Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated,
    Compile / run / mainClass := Some("ptp.streaming.StatusProcessorApp"),
    run / fork := true,
    run / javaOptions ++= sparkJava17Options ++ Seq("-Xmx2g"),
    run / connectInput := false,
    run / outputStrategy := Some(StdoutOutput)
  )

lazy val root = (project in file("."))
  .aggregate(core, streaming)
  .settings(name := "ptp-processor", publish / skip := true)
