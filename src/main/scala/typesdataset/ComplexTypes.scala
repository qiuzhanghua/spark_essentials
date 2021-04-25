package com.example
package typesdataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesWithReleaseDates = moviesDF.select(col("Title"),
    to_date(col("Release_Date"), "d-MMM-yy").as("Actual_Release"))

  moviesWithReleaseDates
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age2", datediff(col("Today"), col("Actual_Release")) / 365)
  //    .show()

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull)
  //    .show()

  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ",")
    .csv("src/main/resources/data/stocks.csv")

  //  stocksDF.show()

  val stockDFWithDates = stocksDF
    .withColumn("actual_date", to_date(col("date"), "MMM d yyyy"))
    //.show()

  moviesDF.select(col("Title"),
    struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
  //    .show()

  moviesDF.selectExpr("Title",
    "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")
  //    .show()

  val movieWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words"))
  //    .show()
  movieWithWords.select(col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  )
    .show()
}
