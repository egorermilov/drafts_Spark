package com.egorermilov.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCountBetter {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WordCountBetter")

    sc.textFile("/home/user/Dropbox/SparkData/SparkScala/book.txt")
      .flatMap(x => x.split("\\W+"))
      .map(x => x.toUpperCase)
      .countByValue()
      .foreach(println)

  }

}
