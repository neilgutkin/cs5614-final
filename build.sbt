name := "Producer"

version := "1.0"

scalaVersion := "2.12.10"
resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.1",
  "org.json4s" %% "json4s-jackson" % "3.6.11"
)