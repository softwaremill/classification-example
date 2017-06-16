package com.softwaremill.blog.tech.classification

import scala.io.Source
import scala.util.Random

object RandomClassification {

  val lines = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("data/abstracts.csv")).getLines()
  val rows = for (line <- lines) yield line.split(";").map(_.trim)

  val (map, _) = rows.foldRight((Map[String, Int](), 0)) { (row, mapAndIndex) =>
    val (map, index) = mapAndIndex
    val category = row(0)

    map.get(category) match {
      case Some(_) => mapAndIndex
      case None => (map + (category -> index), index + 1)
    }
  }

  val categoriesCount = map.keySet.size

  def guess(): Seq[(Int, Int)] = {
    val r = for(row <- rows) yield {
      val category = row(0)
      val categoryIndex = map(category)
      val random = Random.nextInt(categoriesCount)

      (categoryIndex, random)
    }

    r.toSeq
  }

  def calculateCorrectness(category: Seq[Int], predicted: Seq[Int]) = {
    if (category.size != predicted.size) {
      throw new RuntimeException("Wrong")
    }

    val hits = category.zip(predicted).count(p => p._1 == p._2)

    hits.toDouble / category.size
  }

}
