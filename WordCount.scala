package com.egorermilov.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WordCount")

    sc.textFile("/home/user/Dropbox/SparkData/SparkScala/book.txt")
      .flatMap(x => x.split(" "))
      .countByValue()
      .foreach(println)

  }

}
