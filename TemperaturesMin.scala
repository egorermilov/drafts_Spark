package com.egorermilov.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import scala.math.min

object TemperaturesMin {

  def parseLine(line: String) = {
    val fields = line.split(",")
    (fields(0), fields(2), fields(3).toFloat * 0.1f)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "TemperaturesMin")

    sc.textFile("/home/user/Dropbox/SparkData/SparkScala/1800.csv")
      .map(parseLine)
      .filter(x => x._2 == "TMIN")
      .map(x => (x._1, x._3.toFloat))
      .reduceByKey((curr,next) => min(curr,next))
      .collect()
      .foreach(println)

  }

}
