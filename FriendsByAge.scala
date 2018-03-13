package com.egorermilov.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FriendsByAge {

  def parseLine(line: String) = {
    val fields = line.split(",")
    (fields(2).toInt, fields(3).toInt)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")

    sc.textFile("/home/user/Dropbox/SparkData/SparkScala/fakefriends.csv")
      .map(parseLine)
      .mapValues(x => (x, 1))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1 / x._2)
      .collect()
      .sorted
      .foreach(println)


  }

}
