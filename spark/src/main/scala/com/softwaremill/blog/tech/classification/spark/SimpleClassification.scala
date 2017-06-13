package com.softwaremill.blog.tech.classification.spark

import org.apache.spark.ml.{Pipeline, UnaryTransformer}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataType, StringType}

import scala.collection.convert.Wrappers.JListWrapper

object SimpleClassification {

  def main(args: Array[String]) {
    val session = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()

    val data = session.read.option("delimiter", ";").csv("common/src/main/resources/data/abstracts.csv")
    data.foreach { r =>
      // Dirty sanity check
      if (r.size > 3) throw new RuntimeException("Row longer than 3")
    }

    // Concat title and description
    val concat = new SQLTransformer().setStatement("SELECT _c0 AS category, concat(_c1, ' ', _c2) AS titleAndDesc FROM __THIS__")
    val tokenizer = new RegexTokenizer().setPattern("\\W").setInputCol("titleAndDesc").setOutputCol("titleAndDescTokenized")
    val stopWordsRemover = new StopWordsRemover().setInputCol("titleAndDescTokenized").setOutputCol("tokensWithoutStopwords")
    val stem = new Stemmer().setInputCol("tokensWithoutStopwords").setOutputCol("stemmedAsString")
    val tok2 = new RegexTokenizer().setPattern("\\W").setInputCol("stemmedAsString").setOutputCol("stemmed")
    val hashing = new HashingTF().setInputCol("stemmed").setOutputCol("features")
    val si = new StringIndexer().setInputCol("category").setOutputCol("categoryNum")

    val pipeline = new Pipeline().setStages(Array(concat, tokenizer, stopWordsRemover, stem, tok2, hashing, si))
    val ready = pipeline.fit(data).transform(data)

    ready.show(false)

    val results = for(i <- 1 to 10) yield {
      println(i)
      trainAndPredict(ready)
    }

    val average = results.sum / results.size
    val meanSquareError = results.map(d => d * d).sum / results.size
    println(s"Results: average: $average, mean square error: $meanSquareError")

    session.stop()
  }

  def trainAndPredict(data: DataFrame) = {
    val Array(train, test) = data.randomSplit(Array(0.8, 0.2))

    val nb = new NaiveBayes().setLabelCol("categoryNum").setFeaturesCol("features")
    val predictor = nb.fit(train).setFeaturesCol("features").setPredictionCol("predicted").setProbabilityCol("probability")

    val predicted = predictor.transform(test)
    predicted.show()

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("categoryNum").setPredictionCol("predicted")
    evaluator.evaluate(predicted)
  }
}

class Stemmer(override val uid: String) extends UnaryTransformer[Seq[String], String, Stemmer] {

  def this() = this(Identifiable.randomUID("stemming"))

  override protected def createTransformFunc: (Seq[String]) => String = { input =>
    val f = com.databricks.spark.corenlp.functions.lemma.f.asInstanceOf[String => JListWrapper[String]]
    f(input.mkString(" ")).mkString(" ")
  }

  override protected def outputDataType: DataType = StringType
}
