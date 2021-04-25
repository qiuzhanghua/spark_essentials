package com.example
package typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {

  val spark = SparkSession.builder()
    .appName("Common Type")
    .config("spark.master", "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.select(col("Title"),
    col("Rotten_Tomatoes_Rating"), col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  )
//    .show()

  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)
  moviesDF.select("Title", "IMDB_Rating").na.drop()
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
//    .show()
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 0,
    "Director"-> "Unknown",
  ))

  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl",  // same as coalesce
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nulliff", // return null if two is EQUAL, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if first != null second else third
  )
    .show()
}
