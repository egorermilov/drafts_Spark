package com.egorermilov.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object PurchaseByCustomer {

  def parseLine(line: String) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "PurchaseByCustomer")

    val result = sc.textFile("/home/user/Dropbox/SparkData/SparkScala/customer-orders.csv")
      .map(parseLine)
      .reduceByKey((curr, next) => curr+next)
      .map(x => (x._2, x._1))
      .sortByKey()
      .collect()

    result.foreach(println)
  }

}
