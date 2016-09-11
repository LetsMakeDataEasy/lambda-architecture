import sbt._
import Keys._
import sbtassembly._
import sbtassembly.AssemblyKeys._

object LaBuild extends Build {
  name := "lambda-architecture"

  lazy val commonSettings = Seq(
    organization := "com.thoughtworks",
    version := "0.1.0",
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(
      "org.scalatest" % "scalatest_2.11" % "3.0.0" % "test",
      "co.fs2" %% "fs2-core" % "0.9.0-RC2",
      "co.fs2" %% "fs2-io" % "0.9.0-RC2"
    )
  )

  lazy val root = (project in file("."))
    .settings(commonSettings: _*)
    .settings(
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % "0.8.2.1",
      "backtype" % "dfs-datastores-cascading" % "1.2.0",
      "cascalog" % "cascalog" % "1.10.0",
      "log4j" % "log4j" % "1.2.16",
      "org.apache.storm" % "storm-core" % "0.9.4",
      "org.apache.storm" % "storm-kafka" % "0.9.4",
      "backtype" % "cascading-thrift" % "0.2.3" exclude("libthrift", "org.apache.thrift"),
      "elephantdb" % "elephantdb-cascalog" % "0.4.5" exclude("libthrift", "org.apache.thrift"),
      "elephantdb" % "elephantdb-bdb" % "0.4.5" exclude("libthrift", "org.apache.thrift"),
      "com.googlecode.json-simple" % "json-simple" % "1.1",
      "com.clearspring.analytics" % "stream" % "2.7.0",
      "org.hectorclient" % "hector-core" % "2.0-0",
      "org.apache.hadoop" % "hadoop-core" % "0.20.2-dev",
      "org.apache.zookeeper" % "zookeeper" % "3.4.6"
    ))
    .settings(
    resolvers ++= Seq(
      "conjars" at "http://conjars.org/repo",
      "clojars" at "http://clojars.org/repo",
      "oracle" at "http://download.oracle.com/maven"
    ))
    .settings(
    assemblyJarName in assembly := "la.jar",
      mainClass in assembly := Some("com.thoughtworks.la.Main")
  )
}
