package com.softwaremill.blog.tech.classification.elastic

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl.{percolate, _}

import scala.io.Source

class Percolators(client: ElasticClient) {


  def registerQueriesAndPercolateSampleDoc(): Unit = {

    val percolators = List(
      "backend" -> stringQuery("akka OR mongo OR stream OR rdbms"),
      "frontend" -> stringQuery("react OR angular OR javascript OR typescript")
    )

    for {
      (percolatorKey, percolatorQuery) <- percolators
    } {
      client.execute {
        register id percolatorKey into AbstractsIndex.IndexName query percolatorQuery
      }.await
    }

  }

  def percolateAbstracts(n: Int): Unit = {
    val lines = Source.fromInputStream(this.getClass.getResourceAsStream("/data/abstracts.csv")).getLines()
    lines.take(n).map(_.split(';').toList).foreach {
      case label :: title :: text =>
        val response = client.execute {
          percolate in AbstractsIndex.IndexName / AbstractsIndex.TypeName doc(
            "title" -> title,
            "text" -> text
          )
        }.await

        println(s"${response.getMatches.map(_.getId).mkString(", ")} / $label")
      case _ => // do nothing
    }
  }

}
