package com.softwaremill.blog.tech.classification.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SimpleClassification {

  def main(args: Array[String]) {
//    val conf = new SparkConf().setMaster("local[4]").setAppName("Simple Application")
//    val sc = new SparkContext(conf)

    val session = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()

    val r = session.read.option("delimiter", ";").csv("common/data/abstracts.csv")

    r.show()

//    r.transform()
//    val sqlContext = new SQLContext()
//    val abstracts = sc.textFile("common/data/abstracts.csv")
//    val linesAsArrays = abstracts.map(line => line.split(";"))
//    val groupByCategory = linesAsArrays.groupBy(a => a(0))




//    val numAs = abstracts.filter(line => line.contains("a")).count()
//    val numBs = abstracts.filter(line => line.contains("b")).count()
//    println(s"Lines with a: $numAs, Lines with b: $numBs")
//    sc.stop()
  }
}
