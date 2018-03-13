package com.egorermilov.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object PopularMoviesNicer {

  def loadMovieNames(): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("/home/user/Dropbox/SparkData/MovLen/u.item").getLines()

    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }

  def parseLine(line: String) = {
    val fields = line.split("\t")
    fields(1).toInt
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "PopularMoviesNicer")

    val nameDict = sc.broadcast(loadMovieNames)
//    println(loadMovieNames())

    val result = sc.textFile("/home/user/Dropbox/SparkData/MovLen/u.data")
      .map(parseLine)
      .map(x => (x, 1))
      .reduceByKey((curr, next) => curr + next)
      .map(x => (x._2, x._1))
      .sortByKey()
      .map(x => (nameDict.value(x._2), x._1))
      .collect()

    result.foreach(println)
  }

}
