
name := "shashank_maithani_cs441_hw5"

scalaVersion := "2.11.12"
lazy val commonSettings = Seq(
  organization := "Shanks.spark",
  version := "0.1"
)
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
lazy val app = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "PageRankSpark"
  ).
  enablePlugins(AssemblyPlugin)

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" ,
  "org.apache.spark" %% "spark-sql" % "2.4.0" ,
  "com.databricks" %% "spark-xml" % "0.5.0" ,
  "org.apache.hadoop" % "hadoop-client" % "2.8.0" ,
  "org.apache.commons" % "commons-text" % "1.6" ,
  "org.apache.commons" % "commons-csv" % "1.6" ,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test" ,
//  "ch.qos.logback" % "logback-classic" % "1.2.3" ,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
//  "com.typesafe" % "config" % "1.3.2"


)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

excludeDependencies += "org.slf4j.slf4j-jdk14"

//excludeDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "2.4.0",
//  "org.apache.spark" %% "spark-sql" % "2.4.0",
//  "com.databricks" %% "spark-xml" % "0.5.0",
//  "org.apache.hadoop" % "hadoop-client" % "2.8.0"
//)

