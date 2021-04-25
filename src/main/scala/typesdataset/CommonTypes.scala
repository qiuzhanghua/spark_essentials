package com.example
package typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {
  val spark = SparkSession.builder()
    .appName("Common Type")
    .config("spark.master", "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.select(col("Title"), lit(47).as("plain_value"))
  //    .show()

  val dramaFilter = col("Major_Genre") === "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select(col("Title")).where(dramaFilter)
  //    .show()


  val moviesWithGoodnessFlagDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie")).where(dramaFilter)
  //    .show()

  moviesWithGoodnessFlagDF.where("good_movie")
  moviesWithGoodnessFlagDF.where(not(col("good_movie")))

  val moviesAvgRatingDF = moviesDF.select(col("Title"),
    (col("Rotten_Tomatoes_Rating") / 2 + col("IMDB_Rating") / 2).as("Rating"))
  //    .show()

  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))


  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.select(initcap(col("Name"))) // low, upper
  //    .show()

  carsDF.select("*").where(col("Name").contains("volkswagen"))
  //    .show()

  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract"))
    .where(col("regex_extract") =!= "").drop("regex_extract")
  //  vwDF.show()
  vwDF.select(col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace"))
    .show()

  def getCarNames: List[String] = List("volkswagen", "Ford")

  val complexRegex = getCarNames.map(_.toLowerCase).mkString("|")
  carsDF.select(col("Name"), regexp_extract(col("Name"), complexRegex, 0).as("regexp_extract"))
    .where(col("regexp_extract") =!= "").drop("regexp_extract")
  //    .show()

  val carNameFilters = getCarNames.map(_.toLowerCase).map(col("Name").contains(_))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, nextFilter) => combinedFilter or nextFilter)

  carsDF.filter(bigFilter)
  //    .show()


}
