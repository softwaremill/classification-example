package com.softwaremill.blog.tech.classification.spark

import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, GenericRow}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.mutable

object SimpleClassification {

  def main(args: Array[String]) {
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

    // Filter non-words from the column
//    val rt = new RegexTokenizer().setInputCol("stemmed").setOutputCol("words").setPattern("\\W")

    val filterNonWords = udf { text: String =>
      text.split("\\W").mkString(" ")
    }
    val filtered = concatenated.select(
      col("category"),
      col("titleAndDesc"),
      filterNonWords(col("titleAndDesc")).as("filteredTitleAndDesc")
    )

    val stemmed = filtered.select(
      new Column("category"),
      new Column("titleAndDesc"),
      com.databricks.spark.corenlp.functions.lemma.apply(new Column("filteredTitleAndDesc")).as("stemmed"))

    val stopWordsRemover = new StopWordsRemover().setInputCol("stemmed").setOutputCol("filtered")
    val noStopWords = stopWordsRemover.transform(stemmed)

    val hashing = new HashingTF().setInputCol("filtered").setOutputCol("features")
    val hashed = hashing.transform(noStopWords)

    val si = new StringIndexer().setInputCol("category").setOutputCol("categoryNum")
    val indexedCategory = si.fit(hashed).transform(hashed)

    val results = for(i <- 1 to 10) yield {
      println(i)
      trainAndPredict(indexedCategory)
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
