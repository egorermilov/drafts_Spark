package com.egorermilov.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}
import scala.math.sqrt

object MovieSimilaritiesBinary {

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

  def parseRatingLine(line: String) = {
    val fields = line.split("\t")
    (fields(0).toInt, fields(1).toInt)
  }

//  type movieRating = (Int, Double)
  type userRatingPair = (Int, (Int, Int))

  def filterDuplicates(userRatings: userRatingPair): Boolean = {
//    val movieRating1 = userRatings._2._1
//    val movieRating2 = userRatings._2._2
    val movie1 = userRatings._2._1
    val movie2 = userRatings._2._2
    movie1 < movie2
  }

//  def makePairs(userRatings: userRatingPair) = {
//    val movieRating1 = userRatings._2._1
//    val movieRating2 = userRatings._2._2
//    val movie1 = movieRating1._1
//    val movie2 = movieRating2._1
//    val rating1 = movieRating1._2
//    val rating2 = movieRating2._2
//    ((movie1, movie2),(rating1, rating2))
//  }

//  type RatingPair = (Double, Double)
  type RatingPairs =((Int, Int), Iterable[Int])

  def computeCosineSimilarity(ratingPairs: RatingPairs): ((Int, Int),(Double, Int)) = {

    val numPairs = ratingPairs._2.size
    val itemPair = ratingPairs._1

    val numerator = numPairs
    val denumenator = sqrt(1) * sqrt(1)

    var score: Double = 0.0
    if (denumenator != 0.0) {
      score = numerator / denumenator
    }
    (ratingPairs._1, (score, numPairs))
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MovieSimilarities")

    val nameDict = loadMovieNames()

    println("viewCount")
    val movieCount = sc.textFile("/home/user/Dropbox/SparkData/MovLen/u.data")
      .map(x => x.split("\t")(1).toInt)
      .countByValue()
//      .take(5)
//      .foreach(println)
    println(movieCount(645))

    val ratings = sc.textFile("/home/user/Dropbox/SparkData/MovLen/u.data")
      .map(parseRatingLine)

    val similarityMatrix = ratings.join(ratings)
      .filter(filterDuplicates)
      .map(x => (x._2, x._1))
//      .map(makePairs)
      .groupByKey()
//      .map(computeCosineSimilarity)
      .map(x => (x._1, (x._2.size / (sqrt(movieCount(x._1._1)) * sqrt(movieCount(x._1._2))), x._2.size)))
      .cache()
//      .take(10)
//      .foreach(println)

//    if (args.length > 0) {
    if (true){
      val scoreThreshold = 0.5
      val coOccurenceThreshold = 5

//      val movieId: Int = args(0).toInt
      val movieId: Int = 191

      val filteredResults = similarityMatrix.filter(x =>
          {
            val pair = x._1
            val sim = x._2
            (pair._1 == movieId || pair._2 == movieId) && (sim._1 > scoreThreshold) && (sim._2 > coOccurenceThreshold)
          })
        .map(x => (x._2, x._1))
        .sortByKey(ascending = false)
        .take(10)

      println("\n>>Top 10 similar movies for " + nameDict(movieId) + "::")

      for (result <- filteredResults) {
        val sim = result._1
        val pair = result._2

        var similarMovieId = pair._1
        if (similarMovieId == movieId){
          similarMovieId = pair._2
        }

        println(nameDict(similarMovieId) + "\tScore: " + sim._1 + "\tSupport: " + sim._2)
      }


    }
  }

}
