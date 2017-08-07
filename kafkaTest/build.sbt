name := "SparkKafka"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/repo/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
  "org.apache.kafka" %% "kafka-clients" % "0.8.2.1",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1"
)


assemblyMergeStrategy in assembly := {

case PathList("javax", "servlet", xs@_*) => MergeStrategy.last

case PathList("javax", "activation", xs@_*) => MergeStrategy.last

case PathList("org", "apache", xs@_*) => MergeStrategy.last

case PathList("org", "w3c", xs@_*) => MergeStrategy.last

case PathList("com", "google", xs@_*) => MergeStrategy.last

case PathList("com", "codahale", xs@_*) => MergeStrategy.last

case PathList(ps@_*) if ps.last endsWith ".properties" => MergeStrategy.first

case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first

case x =>

val oldStrategy = (assemblyMergeStrategy in assembly).value

oldStrategy(x)

}
