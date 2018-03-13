package com.egorermilov.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FriendsByName {

  def ParseLine(line: String) = {
    val fields = line.split(",")
    (fields(1), fields(3).toInt)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByName")

    sc.textFile("/home/user/Dropbox/SparkData/SparkScala/fakefriends.csv")
      .map(ParseLine)
      .mapValues(x => (x, 1))
      .reduceByKey((curr, next) => (curr._1 + next._1, curr._2 + next._2))
      .mapValues(x => x._1 / x._2)
      .collect()
      .sorted
      .foreach(println)

//    println(rdd.first())

  }

}
