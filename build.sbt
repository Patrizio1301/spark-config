name := "spark-config"

version := "0.1"

scalaVersion :=  "2.11.9"

val sparkVersion = "2.3.1"
val catsVersion="1.6.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.typelevel" %% "cats-core" % "2.0.0-RC1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "com.github.pureconfig" %% "pureconfig" % "0.12.3",
  "org.scalatest" %% "scalatest" % "3.0.8",
  "org.tensorflow" %% "spark-tensorflow-connector" % "1.15.0",
  "junit" % "junit" % "4.13-beta-1")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
