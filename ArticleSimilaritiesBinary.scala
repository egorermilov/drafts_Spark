package com.egorermilov.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}
import scala.math.sqrt

object ArticleSimilaritiesBinary {

  def loadUIDReplacement(): Map[String, Int] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var uidNames: Map[String, Int] = Map()

    val lines = Source.fromFile("/home/user/Dropbox/SparkData/CLASS_articles_autorec.csv").getLines()

    var idCounter = 0
    for (line <- lines) {
      var fields = line.split(',')
      val myVal = fields(0)
      if (!uidNames.keySet.contains(myVal)){
        uidNames += (myVal -> idCounter)
        idCounter += 1
      }

    }
    uidNames
  }

  def parseRatingLine(line: String) = {
    val fields = line.split(",")
//    (fields(0).slice(0,fields(0).length-1).toLong, fields(1).toInt)
    (fields(0), fields(1).toInt)
  }

  //  type movieRating = (Int, Double)

  type userRatingPair = (String, (Int, Int))
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
    val sc = new SparkContext("local[*]", "ArticleSimilaritiesBinary")

    //    val nameDict = loadMovieNames()

    println("viewCount ::")
    val movieCount = sc.textFile("/home/user/Dropbox/SparkData/CLASS_articles_autorec.csv")
      .mapPartitionsWithIndex((i,x) => if (i==0) x.drop(1) else x)
      .map(x => x.split(",")(1).toInt)
      .countByValue()
    //      .take(5)
    //      .foreach(println)
    //    println(movieCount(645))

//    println("uidDict")
//    val uidDict = loadUIDReplacement()
//    println(uidDict)

    println("ratings ::")
    val ratings = sc.textFile("/home/user/Dropbox/SparkData/CLASS_articles_autorec.csv")
      .mapPartitionsWithIndex((i,x) => if (i==0) x.drop(1) else x)
      .map(parseRatingLine)
//      .map(x => (uidDict(x._1), x._2))
//      .take(10)
//      .foreach(println)


            println("similarityMatrix ::")
            val similarityMatrix = ratings.join(ratings)
              .filter(filterDuplicates)
              .map(x => (x._2, x._1))
              .groupByKey()
              .map(x => (x._1, (x._2.size / (sqrt(movieCount(x._1._1)) * sqrt(movieCount(x._1._2))), x._2.size)))
              .cache()
              .take(50)
              .foreach(println)







    //    if (args.length > 0) {
    //    if (true){
    //      val scoreThreshold = 0.5
    //      val coOccurenceThreshold = 5
    //
    ////      val movieId: Int = args(0).toInt
    //      val movieId: Int = 191
    //
    //      val filteredResults = similarityMatrix.filter(x =>
    //          {
    //            val pair = x._1
    //            val sim = x._2
    //            (pair._1 == movieId || pair._2 == movieId) && (sim._1 > scoreThreshold) && (sim._2 > coOccurenceThreshold)
    //          })
    //        .map(x => (x._2, x._1))
    //        .sortByKey(ascending = false)
    //        .take(10)
    //
    //      println("\n>>Top 10 similar movies for " + movieId + "::")
    //
    //      for (result <- filteredResults) {
    //        val sim = result._1
    //        val pair = result._2
    //
    //        var similarMovieId = pair._1
    //        if (similarMovieId == movieId){
    //          similarMovieId = pair._2
    //        }
    //
    //        println(similarMovieId + "\tScore: " + sim._1 + "\tSupport: " + sim._2)
    //      }
    //
    //
    //    }
  }

}
