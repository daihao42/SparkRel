name := "SparkES"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.hadoop" % "hadoop-common" % "2.3.0-cdh5.0.2" % "provided",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "5.5.0"
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
