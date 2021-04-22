package com.example
package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


  // val genresCountDF = moviesDF.select(count(col("Major_Genre")).as("counter"))
  // moviesDF.selectExpr("count(Major_Genre)")
  // count(*)
  // moviesDF.select(countDistinct(col("Major_Genre")).as("counter")).show()
  // moviesDF.select(approx_count_distinct("Major_Genre")).show()

  // moviesDF.select(min("IMDB_Rating")).show()  //selectExpr("min(IMDB_Rating)")
  // moviesDF.select(sum("US_Gross")).show()
  // avg, mean, stddev

  // val countByGenre = moviesDF.groupBy(col("Major_Genre")).count()
  // countByGenre.show()

  // moviesDF.groupBy(col("Major_Genre")).avg("IMDB_Rating").show()

  //  moviesDF.groupBy(col("Major_Genre"))
  //    .agg(
  //      count("*").as("N_Movies"),
  //      avg("IMDB_Rating").as("Avg_Rating")
  //    ).orderBy(col("Avg_Rating")).show()

//  moviesDF.select(sum(expr("US_Gross + Worldwide_Gross + US_DVD_Sales")).as("total")).show()
//  moviesDF.select(countDistinct("Director")).show()
//  moviesDF.agg(
//    mean("US_Gross").as("agv"),
//    stddev("US_Gross").as("stddev"))
//    .show()

  moviesDF.groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()

}
