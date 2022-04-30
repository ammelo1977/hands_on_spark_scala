ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

libraryDependencies ++= Seq( "org.apache.spark" % "spark-core_2.12" % "3.2.1" % "provided")
libraryDependencies ++= Seq( "org.apache.spark" % "spark-sql_2.12" % "3.2.1" % "provided")

lazy val root = (project in file("."))
  .settings(
    name := "hands_on_spark_scala"
  )
