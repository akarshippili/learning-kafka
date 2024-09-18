ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "learning-kafka"
  )

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.8.0",
  "org.apache.kafka" % "kafka-streams" % "3.8.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.8.0")

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core" % "0.14.10",
  "io.circe" %% "circe-parser" % "0.14.10",
  "io.circe" %% "circe-generic" % "0.14.10"
)