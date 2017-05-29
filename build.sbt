import sbt.Keys._

val commonSettings = Seq(
  organization := "com.softwaremill.blog.tech",
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.11.11"
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "classification"
  )
  .aggregate(elasticsearch, spark)

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

lazy val spark = (project in file("spark"))
  .settings(commonSettings)
  .settings(
    name := "classification-spark"
  )
