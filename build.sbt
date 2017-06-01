import sbt.Keys._

val commonSettings = Seq(
  organization := "com.softwaremill.blog.tech",
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.11.11",
  libraryDependencies ++= Seq(
    // Common
    "ch.qos.logback"                    % "logback-classic"                     % "1.1.7",
    "com.typesafe.scala-logging"        %% "scala-logging"                      % "3.5.0"
  )
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "classification"
  )
  .aggregate(elasticsearch, spark)

lazy val common = (project in file("common"))
  .settings(commonSettings)
  .settings(
    name := "common"
  )

lazy val elasticsearch = (project in file("elasticsearch"))
  .settings(commonSettings)
  .settings(
    name := "classification-elasticsearch"
  )
  .settings(
    libraryDependencies ++= {
      val elastic4sV = "2.2.1"

      Seq(
        // Elasticsearch client
        "com.sksamuel.elastic4s"        %% "elastic4s-core"                     % elastic4sV,
        "com.sksamuel.elastic4s"        %% "elastic4s-json4s"                   % elastic4sV,
        // Elastisearch testing / embedded node
        "org.apache.lucene"             %  "lucene-expressions"                 % "4.10.4",
        "org.codehaus.groovy"           %  "groovy-all"                         % "2.4.4",
        "com.github.spullara.mustache.java" % "compiler"                        % "0.9.1",
        "net.java.dev.jna"              %  "jna"                                % "4.2.1"
      )
    }
  )
  .dependsOn(LocalProject("common"))

val sparkVersion = "2.1.1"

lazy val spark = (project in file("spark"))
  .settings(commonSettings)
  .settings(
    name := "classification-spark"
  )
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion
    )
  )
  .dependsOn(LocalProject("common"))
