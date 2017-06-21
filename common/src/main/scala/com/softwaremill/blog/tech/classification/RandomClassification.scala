package com.softwaremill.blog.tech.classification

import scala.util.Random

object RandomClassification {

  def guess(categories: Iterable[String]): Iterable[(Int, Int)] = {
    val map = createCategoriesToIndexMap(categories)

    categories.map { category =>
      val categoryIndex = map(category)
      val random = Random.nextInt(map.size)

      (categoryIndex, random)
    }
  }

  private def createCategoriesToIndexMap(categories: Iterable[String]): Map[String, Int] = {
    val folded = categories.foldRight((Map[String, Int](), 0)) { (category, mapAndIndex) =>
      val (map, index) = mapAndIndex

      map.get(category) match {
        case Some(_) => mapAndIndex
        case None => (map + (category -> index), index + 1)
      }
    }

    folded._1
  }

  def calculateCorrectness(category: Iterable[Int], predicted: Iterable[Int]): Double = {
    if (category.size != predicted.size) {
      throw new RuntimeException("Wrong")
    }

    val hits = category.zip(predicted).count(p => p._1 == p._2)

    hits.toDouble / category.size
  }

}
