package com.softwaremill.blog.tech.classification.spark

import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

object SimpleClassification {

  def main(args: Array[String]) {
//    val conf = new SparkConf().setMaster("local[4]").setAppName("Simple Application")
//    val sc = new SparkContext(conf)

    val session = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()

    val r = session.read.option("delimiter", ";").csv("common/src/main/resources/data/abstracts.csv")
    r.foreach { r =>
      // Dirty sanity check
      if (r.size > 3) throw new RuntimeException("Row longer than 3")
    }

    // Concat title and description
    val concat = new SQLTransformer().setStatement("SELECT _c0 AS category, concat(_c1, ' ', _c2) AS titleAndDesc FROM __THIS__")
    val concatenated = concat.transform(r)

    // Separate words, so we get all the words as an array
    val rt = new RegexTokenizer().setInputCol("titleAndDesc").setOutputCol("words").setPattern("\\W")
    val separated = rt.transform(concatenated)

    val stopWordsRemover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
    val filtered = stopWordsRemover.transform(separated)

    val hashing = new HashingTF().setInputCol("filtered").setOutputCol("features")
    val hashed = hashing.transform(filtered)

    val si = new StringIndexer().setInputCol("category").setOutputCol("categoryNum")
    val indexedCategory = si.fit(hashed).transform(hashed)

    val Array(train, test) = indexedCategory.randomSplit(Array(0.75, 0.25))

    val nb = new NaiveBayes().setLabelCol("categoryNum").setFeaturesCol("features")
    val predictor = nb.fit(train).setFeaturesCol("features").setPredictionCol("predicted").setProbabilityCol("probability")

    val predicted = predictor.transform(test)
    predicted.show()

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("categoryNum").setPredictionCol("predicted")
    val e = evaluator.evaluate(predicted)

    println(e)

    session.stop()
  }
}
