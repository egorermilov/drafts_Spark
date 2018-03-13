package com.egorermilov.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}
import scala.math.max

object PrecipitationMaxDay {

  def parseLine(line: String) = {
    val fields = line.split(",")
    (fields(0), fields(1), fields(2), fields(3).toFloat*0.1f)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "PrecipitationMaxDay")

    sc.textFile("/home/user/Dropbox/SparkData/SparkScala/1800.csv")
      .map(parseLine)
      .filter(x => x._3 == "PRCP")
      .map(x => (x._1, (x._2, x._4)))
      .reduceByKey((curr, next) => if (curr._2 >= next._2){curr} else next)
      .map(x => (x._1, x._2._1, x._2._2))
      .take(10)
      .foreach(println)

  }

}
