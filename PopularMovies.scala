package com.egorermilov.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object PopularMovies {

  def parseLine(line: String) = {
    val fields = line.split("\t")
    fields(1).toInt
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "PopularMovies")

    val result = sc.textFile("/home/user/Dropbox/SparkData/MovLen/u.data")
      .map(parseLine)
      .map(x => (x, 1))
      .reduceByKey((curr, next) => curr + next)
      .map(x => (x._2, x._1))
      .sortByKey()
      .collect()

    result.foreach(println)
  }

}
