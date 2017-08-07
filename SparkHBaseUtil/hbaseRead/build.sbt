name := "HbaseReader"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.3.0-cdh5.0.2" % "provided",
  "org.apache.hbase" % "hbase-client" % "0.96.1.1-cdh5.0.2" % "provided",
  "org.apache.hbase" % "hbase-common" % "0.96.1.1-cdh5.0.2" % "provided",
  "org.apache.hbase" % "hbase-server" % "0.96.1.1-cdh5.0.2" % "provided"
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

case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first

case x =>

val oldStrategy = (assemblyMergeStrategy in assembly).value

oldStrategy(x)

}
