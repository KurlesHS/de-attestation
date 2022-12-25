version in ThisBuild := "0.1.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "test_task",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.2",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.2"
  )