import sbt.{Credentials, Path}

name := "spark-streaming"
organization := "com.spark"
version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

resolvers ++= Seq(
  "kafka-connect-avro-converter" at "http://packages.confluent.io/maven/",
  "maven Repo" at "https://mvnrepository.com/artifact/com.google.guava/guava"
)

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
lazy val excludeSlf4j = ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")
lazy val excludeLog4j = ExclusionRule(organization = "log4j", name = "log4j")

lazy val dataProcessingCommon = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := name.value,
    organization := organization.value,
    version := version.value,
    assemblyJarName in assembly := s"spark-streaming-${version.value}.jar",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-hive" % sparkVersion,
      "org.apache.spark" %% "spark-yarn" % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "za.co.absa" %% "abris" % "3.0.3.2",
      "org.scala-lang" % "scala-compiler" % scalaVersion.value,
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "com.typesafe" % "config" % "1.3.4",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
      "org.yaml" % "snakeyaml" % "1.25",
      "net.jpountz.lz4" % "lz4" % "1.3.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.0",
      "postgresql" % "postgresql" % "9.1-901-1.jdbc4",
      "org.apache.hadoop" % "hadoop-aws" % "2.7.4",
      "com.amazonaws" % "aws-java-sdk" % "1.7.4",
      "com.typesafe.akka" %% "akka-actor" % "2.5.25" excludeAll (excludeJpountz),
      "junit" % "junit" % "4.12" % Test,
      "org.scalatest" %% "scalatest" % "3.0.5",
      "com.banzaicloud" %% "spark-metrics" % "2.3-2.1.0.2",
      "io.prometheus" % "simpleclient" % "0.8.0",
      "io.prometheus" % "simpleclient_dropwizard" % "0.8.0",
      "io.prometheus" % "simpleclient_pushgateway" % "0.8.0",
      "io.dropwizard.metrics" % "metrics-core" % "4.1.1",
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.2"
    ),
    libraryDependencies ~= {
      _.map(_.excludeAll(excludeSlf4j))
    },
    assemblyOption in assembly ~= {
      _.copy(includeScala = true)
    },
    assemblyMergeStrategy in assembly := {
      case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
      case PathList("META-INF", xs @ _*)                                       => MergeStrategy.discard
      case x                                                                   => MergeStrategy.first
    }
  )
