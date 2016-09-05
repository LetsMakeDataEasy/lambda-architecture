name := "lambda-architecture"

lazy val commonSettings = Seq(
  organization := "com.thoughtworks",
  version := "0.1.0",
  scalaVersion := "2.11.8",
  libraryDependencies ++= Seq(
    "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test",
    "co.fs2" %% "fs2-core" % "0.9.0-RC2",
    "co.fs2" %% "fs2-io" % "0.9.0-RC2"
  )
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "0.10.0.0"
    ))
  .settings(
     assemblyJarName in assembly := "la.jar",
     mainClass in assembly := Some("com.thoughtworks.la.Main")
)
