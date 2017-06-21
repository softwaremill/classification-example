package com.softwaremill.blog.tech.classification.spark

import com.softwaremill.blog.tech.classification.RandomClassification
import org.apache.spark.ml.{Pipeline, UnaryTransformer}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, NaiveBayes}
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

    evaluatePipelines(data, session, bayes, regression, decisionTree)

    session.stop()
  }

  def evaluatePipelines(data: DataFrame, session: SparkSession, pipelines: Pipeline*) = {

    val testsForPipelines = (for(i <- 1 to 50) yield {
      println(s"Iteration $i")
      val Array(train, test) = data.randomSplit(Array(0.8, 0.2))
      trainAndPredict(train, test, pipelines, session)
    }).flatten

    val mapped = testsForPipelines.groupBy(_._1).mapValues(_.map(_._2))
    println(mapped)

    val averaged = mapped.mapValues { correctnessSeq =>
      val averageCorrectness = correctnessSeq.sum / correctnessSeq.size
      val sqrDeviationFromAverage = correctnessSeq.map(c => Math.pow(c - averageCorrectness, 2))
      val meanDevFromAverage = sqrDeviationFromAverage.sum / sqrDeviationFromAverage.size
      (averageCorrectness, meanDevFromAverage)
    }

    averaged.foreach { p =>
      println(s"Results for ${p._1}: average: ${p._2._1}, mean square deviation from average: ${p._2._2}")
    }
  }

  def trainAndPredict(train: DataFrame, test: DataFrame, pipelines: Seq[Pipeline], session: SparkSession): Seq[(String, Double)] = {
    val correctnessPerPipeline = for(p <- pipelines) yield {
      println(s"Running pipeline ${p.getStages.last.uid}")
      val result = p.fit(train).transform(test)

      val hit = new SQLTransformer().setStatement("SELECT categoryNum, predicted FROM __THIS__ WHERE categoryNum == predicted").transform(result)
      val miss = new SQLTransformer().setStatement("SELECT categoryNum, predicted FROM __THIS__ WHERE categoryNum != predicted").transform(result)

      val correctenss = hit.count().toDouble / (hit.count() + miss.count())

      println(s"Correctness: $correctenss")

      (p.getStages.last.uid, correctenss)
    }

    val randomCorrectness = randomClassificationCorrectness(test, session)

    correctnessPerPipeline :+ ("random", randomCorrectness)
  }

  def randomClassificationCorrectness(test: DataFrame, session: SparkSession) = {
    import session.implicits._
    import scala.collection.JavaConverters._

    val categories = test.map(r => r.getAs[String]("_c0")).toLocalIterator().asScala
    val g = RandomClassification.guess(categories.toIterable)
    val (category, random) = g.unzip

    RandomClassification.calculateCorrectness(category, random)
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
