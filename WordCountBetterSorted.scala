package com.egorermilov.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCountBetterSorted {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WordCountBetterSorted")

    val result = sc.textFile("/home/egorermilov/Dropbox/SparkData/SparkScala/book.txt")
      .flatMap(x => x.split("\\W+"))
      .map(x => x.toUpperCase)
      .filter(x => !List("YOU","TO", "THE", "A","YOUR").contains(x))
      .map(x => (x, 1))
      .reduceByKey((curr, next) => curr + next)
      .map(x => (x._2, x._1))
      .sortByKey()
      .collect()

    result.foreach(println)
  }


}
