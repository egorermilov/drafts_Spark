package com.egorermilov.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import scala.math.max

object TemperaturesMax {

  def parseLine(line: String) = {
    val fields = line.split(",")
    (fields(0), fields(2), fields(3).toFloat*0.1f)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "TemperaturesMax")

    sc.textFile("/home/user/Dropbox/SparkData/SparkScala/1800.csv")
      .map(parseLine)
      .filter(x => x._2 == "TMAX")
      .map(x => (x._1, x._3))
      .reduceByKey((curr, next) => max(curr,next))
      .take(10)
      .foreach(println)

  }

}
