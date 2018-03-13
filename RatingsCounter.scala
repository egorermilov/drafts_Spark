package com.egorermilov.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RatingsCounter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    def sc = new SparkContext("local[*]", "RatingsCounter")
    val file1 = "file:///home/user/Documents/Spark_data/MovLen/u.data"
    val file2 = "file:///home/user/Documents/Spark_data/flights.csv"

    sc.textFile(file2)
      .map(x => x.toString.split(",")(7))
      .countByValue()
      .toSeq
      .sortBy(_._2)
      .foreach(println)
  }
}
