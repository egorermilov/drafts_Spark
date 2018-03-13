package com.egorermilov.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object MostPopularSuperhero {

  def parseNames(line: String): Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      Some(fields(0).trim().toInt, fields(1))
    } else {
      None
    }
  }

  def loadHeroNames(): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var heroNames: Map[Int, String] = Map()
    val lines = Source
      .fromFile("/home/user/Dropbox/SparkData/SparkScala/Marvel-names.txt")
      .getLines()

    for (line <- lines) {
      val fields = line.split('\"')
      if (fields.length > 1) {
        heroNames += (fields(0).trim.toInt -> fields(1))
      }
    }

    heroNames

  }

  def countCoOccurences(line: String) = {
    var elements = line.split("\\s+")
    (elements(0).toInt, elements.length-1)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")

//    val names = sc.textFile("/home/user/Dropbox/SparkData/SparkScala/Marvel-names.txt")
//      .flatMap(parseNames)
    val namesDict = sc.broadcast(loadHeroNames)

    val result = sc.textFile("/home/user/Dropbox/SparkData/SparkScala/Marvel-graph.txt")
      .map(countCoOccurences)
      .reduceByKey((curr, next) => curr+next)
      .map(x => (x._2, x._1))
      .sortByKey(ascending = false)
      .take(10)
      .map(x => (namesDict.value(x._2), x._1))
      .foreach(println)

//    val mostPopular = names.lookup(result)(0)

//    println(mostPopular)

  }
}
