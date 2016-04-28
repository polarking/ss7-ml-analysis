name := "ss7-ml-analysis"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.1",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.1",
  "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.3.0",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.1",
  "org.cloudera.spark.streaming.kafka" % "spark-kafka-writer" % "0.1.0",
  "org.scala-lang" % "scala-compiler" % "2.10.6",
  "org.scala-lang" % "scala-reflect" % "2.10.6",
  "org.scala-lang" % "scala-library" % "2.10.6",
  "com.sun.xml.bind" % "jaxb-impl" % "2.2.3-1",
  "javax.xml.bind" % "jaxb-api" % "2.2.7",
  "commons-net" % "commons-net" % "2.2",
  "org.apache.commons" % "commons-lang3" % "3.3.2",
  "commons-codec" % "commons-codec" % "1.5",
  "org.slf4j" % "slf4j-api" % "1.7.10"
)

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
