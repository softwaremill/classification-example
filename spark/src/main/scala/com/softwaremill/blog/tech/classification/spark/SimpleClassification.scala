package com.softwaremill.blog.tech.classification.spark

import org.apache.spark.ml.{Pipeline, UnaryTransformer}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, NaiveBayes, RandomForestClassifier}
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

    val data = session.read.option("delimiter", ";").csv("../common/src/main/resources/data/abstracts.csv").cache()
    data.foreach { r =>
      // Dirty sanity check
      if (r.size > 3) throw new RuntimeException("Row longer than 3")
    }

    // Concat title and description
    val concat = new SQLTransformer().setStatement("SELECT _c0 AS category, concat(_c1, ' ', _c2) AS titleAndDesc FROM __THIS__")
    val tokenizer = new RegexTokenizer().setPattern("\\W").setInputCol("titleAndDesc").setOutputCol("titleAndDescTokenized")
    val stopWordsRemover = new StopWordsRemover().setInputCol(tokenizer.getOutputCol).setOutputCol("tokensWithoutStopwords")
    val stem = new Stemmer().setInputCol(stopWordsRemover.getOutputCol).setOutputCol("stemmedAsString")
    val tok2 = new RegexTokenizer().setPattern("\\W").setInputCol(stem.getOutputCol).setOutputCol("stemmed")
    val hashing = new HashingTF().setInputCol(tok2.getOutputCol).setOutputCol("features")
    val si = new StringIndexer().setInputCol("category").setOutputCol("categoryNum")

    val nb = new NaiveBayes().setLabelCol("categoryNum").setFeaturesCol("features").setPredictionCol("predicted").setProbabilityCol("probability")
    val lr = new LogisticRegression().setLabelCol("categoryNum").setFeaturesCol("features").setPredictionCol("predicted").setProbabilityCol("probability").setMaxIter(100)
    val dtc = new DecisionTreeClassifier().setLabelCol("categoryNum").setFeaturesCol("features").setPredictionCol("predicted").setProbabilityCol("probability")
//    val rfc = new RandomForestClassifier().setLabelCol("categoryNum").setFeaturesCol("features").setPredictionCol("predicted").setProbabilityCol("probability")

    val bayes = new Pipeline().setStages(Array(concat, tokenizer, stopWordsRemover, stem, tok2, hashing, si, nb))
    val regression = new Pipeline().setStages(Array(concat, tokenizer, stopWordsRemover, stem, tok2, hashing, si, lr))
    val decisionTree = new Pipeline().setStages(Array(concat, tokenizer, stopWordsRemover, stem, tok2, hashing, si, dtc))
//    val forest = new Pipeline().setStages(Array(concat, tokenizer, stopWordsRemover, stem, tok2, hashing, si, rfc))

    evaluatePipeline(bayes, data)
    evaluatePipeline(regression, data)
    evaluatePipeline(decisionTree, data)
//    evaluatePipeline(forest, data)

    session.stop()
  }

  def evaluatePipeline(pipeline: Pipeline, data: DataFrame) = {
    val pipelineName = pipeline.getStages.last.uid
    println(s"Pipeline: $pipelineName")

    val results = for(i <- 1 to 10) yield {
      trainAndPredict(data, pipeline)
    }

    val average = results.sum / results.size
    val meanSquareError = results.map(d => d * d).sum / results.size
    println(s"Results for $pipelineName: average: $average, mean square error: $meanSquareError")
  }

  def trainAndPredict(data: DataFrame, pipeline: Pipeline) = {
    val Array(train, test) = data.randomSplit(Array(0.8, 0.2))

    val result = pipeline.fit(train).transform(test)

//    result.show(false)

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("categoryNum").setPredictionCol("predicted")
    val evaluation = evaluator.evaluate(result)

//    println(s"Evaluated: $evaluation")

    evaluation
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
