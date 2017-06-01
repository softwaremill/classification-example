package com.softwaremill.blog.tech.classification.elastic.runner

import java.nio.file.Files
import java.util.UUID

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.analyzers.AnalyzerDefinition
import com.sksamuel.elastic4s.mappings.MappingDefinition
import com.typesafe.scalalogging.StrictLogging
import org.elasticsearch.client.Requests
import org.elasticsearch.common.Priority
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.NodeBuilder
import org.json4s.JsonAST.{JArray, JString}
import org.json4s.jackson.JsonMethods._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.{Source, StdIn}
import scala.util.{Failure, Success, Try}

trait StandaloneElasticServer extends StrictLogging {

  protected def port: Int

  protected def indexName: String

  protected def indexType: String

  val elasticHome = Files.createTempDirectory("elastic")

  private lazy val settings = Settings.builder().put("path.home", elasticHome.toAbsolutePath)
  private lazy val node = NodeBuilder.nodeBuilder().settings(settings).node()
  lazy val client: ElasticClient = ElasticClient.fromClient(node.client())

  def start(): Unit = {
    node.start()
  }

  def stop(): Unit = {
    client.close()
    node.close()
  }

  def isRunning: Boolean = !node.isClosed

  def ensureGreen(): Boolean = {
    val healthResponse = client.admin.cluster()
      .health(Requests.clusterHealthRequest(indexName)
        .waitForRelocatingShards(0)
        .waitForGreenStatus()
        .waitForEvents(Priority.LANGUID)
      ).actionGet()

    if (healthResponse.isTimedOut) {
      val state = client.admin.cluster().prepareState().get().getState.prettyPrint()
      val pendingTasks = client.admin.cluster().preparePendingClusterTasks().get().prettyPrint()
      logger.error(s"ensureGreen timed out, cluster state:\n${healthResponse.getStatus}\n$state\n$pendingTasks")
      false
    } else {
      logger.info(s"Index '$indexName' is in the green state.")
      true
    }
  }

}

trait StandaloneElasticServerWithData extends StandaloneElasticServer {

  protected def mapping: MappingDefinition

  protected def customAnalyzers: List[AnalyzerDefinition]

  protected def dataJsonFilePath: String

  override def start(): Unit = {
    super.start()
    deleteIndexIfExists()
    createIndexWithMapping(mapping)
    populateData(dataJsonFilePath)
  }

  override def stop(): Unit = {
    deleteIndexIfExists()
    super.stop()
  }

  private def deleteIndexIfExists(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    client.execute {
      indexExists(indexName)
    }.flatMap {
      case existsResp if existsResp.isExists =>
        client.execute {
          delete index indexName
        }
      case oth => Future.successful(oth)
    }.await(10.seconds)
  }

  private def createIndexWithMapping(mapping: MappingDefinition): Unit = {
    client.execute {
      createIndex(indexName) mappings mapping analysis customAnalyzers shards 1 replicas 0
    }.await(10.seconds)
  }

  private def populateData(sourceFile: String): Unit = {
    val dataSeq = parse(loadResource(sourceFile)) match {
      case JArray(xs) => xs
      case _ => Nil
    }
    logger.info(s"Populating data with ${dataSeq.size} sample docs")
    client.execute {
      bulk {
        dataSeq.map { d =>
          val docId = d \ "id" match {
            case JString(id) => id
            case _ => UUID.randomUUID().toString
          }
          index into indexName / indexType source compact(d) id docId
        }
      } refresh true
    }.await(20.seconds)
  }

  private def loadResource(file: String): String = {
    logger.info(s"Loading $file...")
    Try(classOf[StandaloneElasticServerWithData].getResourceAsStream(file)) match {
      case Success(null) =>
        throw new IllegalArgumentException(s"Cannot find resource: $file")
      case Failure(ex) =>
        throw new IllegalStateException(s"Cannot load resource: $file", ex)
      case Success(resource) =>
        Source.fromInputStream(resource).getLines().mkString("\n")
    }
  }

}

trait ElasticServerRunner extends StrictLogging {

  def elasticServer: StandaloneElasticServer

  def performActions(client: ElasticClient): Unit = ()

  def main(args: Array[String]): Unit = {
    start() match {
      case Success(_) =>
        println(s"\n\n\nStandalone Elasticsearch node started.\n\n\n")
        performActions(elasticServer.client)
        println(s"\n\n\nPress [ENTER] to stop.\n\n\n")
        StdIn.readLine()
        stop()
      case _ => stop() // Do nothing
    }
  }

  def start(): Try[Unit] = {
    val res = Try {
      elasticServer.start()
      if (!elasticServer.ensureGreen()) {
        throw new IllegalStateException(s"Test cluster did not reach expected state.")
      }
    }
    res match {
      case Failure(ex) =>
        logger.error(s"Cannot start local elastic server.", ex)
        stop()
      case _ =>
        logger.info("Elastic server started.")
    }
    res
  }

  def stop(): Unit = {
    if (elasticServer.isRunning) {
      Try(elasticServer.stop()) match {
        case Success(_) =>
          logger.info("Elastic server stopped")
        case Failure(ex) =>
          logger.error(s"Cannot stop local elastic server.", ex)
          System.exit(-1)
      }
    }
  }

}
