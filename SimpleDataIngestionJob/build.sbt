name := "SimpleDataIngestionJob"

version := "0.1"

scalaVersion := "2.12.0"

libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.0" % "test"
libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "2.4.6"
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "2.4.6"
