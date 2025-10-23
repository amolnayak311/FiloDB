import sbt._

object Dependencies {

  // Zookeeper pulls in slf4j-log4j12 which we DON'T want
  val excludeZK = ExclusionRule(organization = "org.apache.zookeeper")
  // This one is brought by Spark by default
  val excludeSlf4jLog4j = ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")
  val excludeJersey = ExclusionRule(organization = "com.sun.jersey")
  // The default minlog only logs to STDOUT.  We want to log to SLF4J.
  val excludeMinlog = ExclusionRule(organization = "com.esotericsoftware", name = "minlog")
  val excludeOldLz4 = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
  val excludeNetty  = ExclusionRule(organization = "io.netty", name = "netty-handler")
  val excludeXBean = ExclusionRule(organization = "org.apache.xbean", name = "xbean-asm6-shaded")
  val excludegrpc = ExclusionRule(organization = "io.grpc")


  /* Versions in various modules versus one area of build */
  val pekkoVersion      = "1.0.3"
  val pekkoHttpVersion  = "1.0.1"
  val cassDriverVersion = "3.7.1"
  val ficusVersion      = "1.3.4"
  val kamonBundleVersion = "2.7.3"
  val monixKafkaVersion = "1.0.0-RC6"
  val sparkVersion      = "2.4.8"
  val sttpVersion       = "1.3.3"

  /* Dependencies shared */
  val logbackDep        = "ch.qos.logback"             % "logback-classic"       % "1.5.6"
  val log4jDep          = "log4j"                      % "log4j"                 % "1.2.17"
  val scalaLoggingDep   = "com.typesafe.scala-logging" %% "scala-logging"        % "3.7.2"
  val scalaTest         = "org.scalatest"              %% "scalatest"            % "3.1.2"
  val scalaCheck        = "org.scalacheck"             %% "scalacheck"           % "1.14.3"
  val scalaTestPlus     = "org.scalatestplus"          %% "scalacheck-1-14"      % "3.1.2.0"
  val pekkoHttp         = "org.apache.pekko"           %% "pekko-http"           % pekkoHttpVersion withJavadoc()
  val pekkoHttpTestkit  = "org.apache.pekko"           %% "pekko-http-testkit"   % pekkoHttpVersion withJavadoc()
  val pekkoHttpCirce    = "de.heikoseeberger"          %% "akka-http-circe"     % "1.21.0"
  val circeGeneric      = "io.circe"                   %% "circe-generic"        % "0.9.3"
  val circeParser       = "io.circe"                   %% "circe-parser"         % "0.9.3"

  lazy val commonDeps = Seq(
    "io.kamon" %% "kamon-bundle"  % kamonBundleVersion,
    "io.kamon" %% "kamon-testkit" % kamonBundleVersion % Test,
    logbackDep % Test,
    scalaTest  % Test,
    "com.softwaremill.quicklens" %% "quicklens" % "1.4.12" % Test,
    "org.apache.xbean" % "xbean-asm6-shaded" % "4.10" % Test,
    scalaCheck % Test,
    scalaTestPlus % Test
  )

  lazy val memoryDeps = commonDeps ++ Seq(
    "com.github.jnr"       %  "jnr-ffi"          % "2.2.12",
    "joda-time"            % "joda-time"         % "2.2" withJavadoc(),
    "org.joda"             % "joda-convert"      % "1.2",
    "org.lz4"              %  "lz4-java"         % "1.4",
    "org.agrona"           %  "agrona"           % "0.9.35",
    "org.jctools"          % "jctools-core"      % "4.0.3" withJavadoc(),
    "org.spire-math"       %% "debox"            % "0.8.0" withJavadoc(),
    scalaLoggingDep
  )

  lazy val coreDeps = commonDeps ++ Seq(
    scalaLoggingDep,
    "io.kamon"                     %% "kamon-zipkin"      % kamonBundleVersion,
    "io.kamon"                     %% "kamon-opentelemetry" % kamonBundleVersion excludeAll(excludegrpc),
    "org.slf4j"                    % "slf4j-api"          % "1.7.10",
    "com.beachape"                 %% "enumeratum"        % "1.5.10",
    "io.monix"                     %% "monix"             % "3.4.0",
    "com.googlecode.concurrentlinkedhashmap"              % "concurrentlinkedhashmap-lru" % "1.4",
    "com.iheart"                   %% "ficus"             % ficusVersion,
    "io.fastjson"                  % "boon"               % "0.33",
    "com.googlecode.javaewah"      % "JavaEWAH"           % "1.1.6" withJavadoc(),
    "com.github.rholder.fauxflake" % "fauxflake-core"     % "1.1.0",
    "org.scalactic"                %% "scalactic"         % "3.2.0" withJavadoc(),
    "org.apache.lucene"            % "lucene-core"        % "9.7.0" withJavadoc(),
    "org.apache.lucene"            % "lucene-facet"       % "9.7.0" withJavadoc(),
    "com.github.alexandrnikitin"   %% "bloom-filter"      % "0.11.0",
    "org.rocksdb"                  % "rocksdbjni"         % "6.29.5",
    "com.esotericsoftware"         % "kryo"               % "4.0.0" excludeAll(excludeMinlog),
    "com.dorkbox"                  % "MinLog-SLF4J"       % "1.12",
    "com.github.ben-manes.caffeine" % "caffeine"          % "3.0.5",
    "com.twitter"                  %% "chill"             % "0.9.3",
    "org.apache.commons"           % "commons-lang3"      % "3.14.0"
  )

  lazy val sparkJobsDeps = commonDeps ++ Seq(
    "org.apache.spark"       %%      "spark-core" % sparkVersion % Provided,
    "org.apache.spark"       %%      "spark-sql"  % sparkVersion % Provided,
    "org.apache.spark"       %%      "spark-core" % sparkVersion % Test excludeAll(excludeNetty, excludeXBean),
    "org.apache.spark"       %%      "spark-sql"  % sparkVersion % Test excludeAll(excludeNetty)
  )

  lazy val cassDeps = commonDeps ++ Seq(
    // other dependencies separated by commas
    "org.lz4"                %  "lz4-java"             % "1.4",
    "com.datastax.cassandra" % "cassandra-driver-core" % cassDriverVersion,
    logbackDep % Test
  )

  lazy val queryDeps = commonDeps ++ Seq(
    "org.apache.pekko"         %% "pekko-actor"                          % pekkoVersion,
    "com.tdunning"            % "t-digest"                              % "3.1",
    "com.softwaremill.sttp"   %% "circe"                                % sttpVersion ,
    "com.softwaremill.sttp"   %% "async-http-client-backend-future"     % sttpVersion,
    "com.softwaremill.sttp"   %% "core"                                 % sttpVersion,
    "org.apache.datasketches" % "datasketches-java"                     % "3.0.0",
    circeGeneric
  )

  lazy val coordDeps = commonDeps ++ Seq(
    "org.apache.pekko"       %% "pekko-cluster-tools"         % pekkoVersion,
    "org.apache.pekko"       %% "pekko-slf4j"                 % pekkoVersion,
    "org.apache.pekko"       %% "pekko-cluster"               % pekkoVersion withJavadoc(),
    "org.apache.pekko"       %% "pekko-management-cluster-bootstrap" % "1.0.0",
    "org.apache.pekko"       %% "pekko-discovery"             % pekkoVersion,
    "io.altoo"               %% "pekko-kryo-serialization"    % "1.0.1" excludeAll(excludeMinlog, excludeOldLz4),
    "de.javakaffee"          % "kryo-serializers"             % "0.42" excludeAll(excludeMinlog),
    "io.kamon"               %% "kamon-prometheus"            % kamonBundleVersion,
    // Redirect minlog logs to SLF4J
    "com.dorkbox"            % "MinLog-SLF4J"                 % "1.12",
    "com.opencsv"            % "opencsv"                      % "3.3",
    "org.apache.pekko"       %% "pekko-testkit"               % pekkoVersion % Test,
    "org.apache.pekko"       %% "pekko-multi-node-testkit"    % pekkoVersion % Test,
    "org.apache.commons" % "commons-text" % "1.9"
  )

  lazy val cliDeps = Seq(
    logbackDep,
    "io.kamon"          %% "kamon-bundle"        % kamonBundleVersion,
    "org.rogach"        %% "scallop"             % "3.1.1"
  )

  lazy val kafkaDeps = Seq(
    "io.monix"          %% "monix-kafka-1x" % monixKafkaVersion,
    "org.apache.kafka"  % "kafka-clients"   % "1.0.0"     % "compile,test" exclude("org.slf4j", "slf4j-log4j12"),
    "org.apache.pekko"  %% "pekko-testkit"  % pekkoVersion % "test,it",
    scalaTest  % "test,it",
    logbackDep % "test,it")

  lazy val promDeps = Seq(
    "com.google.protobuf"    % "protobuf-java"             % "2.5.0",
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.1",
    "com.softwaremill.quicklens" %% "quicklens"            % "1.4.12",
    "org.antlr" % "antlr4-runtime" % "4.9.1"
  )

  lazy val gatewayDeps = commonDeps ++ Seq(
    logbackDep,
    "io.monix"   %% "monix-kafka-1x" % monixKafkaVersion,
    "org.rogach" %% "scallop"        % "3.1.1",
    "io.netty" % "netty" % "3.10.6.Final"
  )

  lazy val httpDeps = Seq(
    logbackDep,
    pekkoHttp,
    pekkoHttpCirce,
    circeGeneric,
    circeParser,
    pekkoHttpTestkit % Test,
    "org.xerial.snappy" % "snappy-java" % "1.1.8.4"
  )

  lazy val standaloneDeps = Seq(
    logbackDep,
    "io.kamon"              %% "kamon-zipkin"            % kamonBundleVersion,
    "com.iheart"            %% "ficus"                   % ficusVersion      % Test,
    "org.apache.pekko"      %% "pekko-multi-node-testkit" % pekkoVersion     % Test,
    "com.softwaremill.sttp" %% "circe"                   % sttpVersion       % Test,
    "com.softwaremill.sttp" %% "akka-http-backend"       % sttpVersion       % Test,
    "com.softwaremill.sttp" %% "core"                    % sttpVersion       % Test,
    "org.apache.pekko"      %% "pekko-stream"            % pekkoVersion      % Test
  )

  // Removed bootstrapperDeps - no longer needed with Pekko built-in cluster bootstrap

  //  lazy val sparkDeps = Seq(
  //    // We don't want LOG4J.  We want Logback!  The excludeZK is to help with a conflict re Coursier plugin.
  //    "org.apache.spark" %% "spark-hive"              % sparkVersion % "provided" excludeAll(excludeSlf4jLog4j, excludeZK),
  //    "org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion % "provided" excludeAll(excludeSlf4jLog4j, excludeZK),
  //    "org.apache.spark" %% "spark-streaming"         % sparkVersion % "provided",
  //    scalaTest % "it"
  //  )

  lazy val jmhDeps = Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion excludeAll(excludeSlf4jLog4j, excludeZK, excludeJersey),
    "org.apache.pekko"       %% "pekko-testkit"               % pekkoVersion
  )

  lazy val gatlingDeps = Seq(
      "io.gatling.highcharts" % "gatling-charts-highcharts" % "3.2.0" % "test,it",
      "io.gatling"            % "gatling-test-framework"    % "3.2.0" % "test,it"
  )

  lazy val pekkoBootstrapDeps = Seq(
    "org.apache.pekko"       %% "pekko-actor"             % pekkoVersion,
    "org.apache.pekko"       %% "pekko-cluster"           % pekkoVersion,
    "org.apache.pekko"       %% "pekko-http"              % pekkoHttpVersion,
    "de.heikoseeberger"      %% "akka-http-circe"         % "1.21.0",
    "io.circe"               %% "circe-generic"           % "0.9.3",
    "io.circe"               %% "circe-parser"            % "0.9.3",
    scalaLoggingDep,
    scalaTest % Test
  )

  //  lazy val stressDeps = Seq(
  //    "com.databricks"       %% "spark-csv"         % "1.3.0",
  //    scalaxyDep,
  //    "org.apache.spark"     %% "spark-sql"         % sparkVersion % "provided" excludeAll(excludeZK),
  //    "org.apache.spark"     %% "spark-streaming"   % sparkVersion % "provided" excludeAll(excludeZK)
  //  )
}
