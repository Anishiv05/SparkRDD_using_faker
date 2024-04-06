ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "DataAnalysis_with_Spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.0",
      "com.github.javafaker" % "javafaker" % "1.0.2",
      "org.apache.spark" %% "spark-sql" % "3.2.0",
      "org.json4s" %% "json4s-jackson" % "3.7.0"
    )
  )