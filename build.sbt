ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "bdeProject",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.1",
      "org.apache.spark" %% "spark-sql" % "3.2.1",
      "org.apache.spark" %% "spark-mllib" % "3.2.1",
      "org.apache.spark" %% "spark-streaming" % "3.2.1",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.2.1",
      "org.apache.kafka" % "kafka-clients" % "2.8.1")
  )
