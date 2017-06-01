package com.softwaremill.blog.tech.classification.elastic.runner

import com.sksamuel.elastic4s.ElasticClient
import com.softwaremill.blog.tech.classification.elastic.{AbstractsIndex, Percolators}

object AbstractsElasticServerRunner extends ElasticServerRunner {

  override def elasticServer: StandaloneElasticServer = new AbstractsElasticServer(9200)

  override def performActions(client: ElasticClient): Unit = {
    val percolators = new Percolators(client)
    percolators.registerQueriesAndPercolateSampleDoc()
    percolators.percolateAbstracts(10)
  }
}

class AbstractsElasticServer(val port: Int) extends StandaloneElasticServerWithData {
  override protected val indexName = AbstractsIndex.IndexName
  override protected val indexType = AbstractsIndex.TypeName
  override protected val mapping = AbstractsIndex.Mapping
  override protected val customAnalyzers = Nil
  override protected val dataJsonFilePath = "/data/abstracts.json"
}



