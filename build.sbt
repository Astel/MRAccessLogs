
name := "MRAccessLogs"

version := "0.1"

scalaVersion := "2.12.5"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "3.1.0",
  "eu.bitwalker" % "UserAgentUtils" % "1.21",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "org.mockito" % "mockito-core" % "2.8.47" % Test
)
