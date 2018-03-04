name := "Spark PageRank"

version := "0.1"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
)