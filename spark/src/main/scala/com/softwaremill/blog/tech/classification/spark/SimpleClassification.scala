package com.softwaremill.blog.tech.classification.spark

import org.apache.spark.{SparkConf, SparkContext}

object SimpleClassification {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val abstracts = sc.textFile("common/data/abstracts.csv")
    val numAs = abstracts.filter(line => line.contains("a")).count()
    val numBs = abstracts.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()
  }
}
