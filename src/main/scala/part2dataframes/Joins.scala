package com.example
package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  val joinCondition = guitaristDF.col("band") === bandsDF.col("id")
  val guitaristBandsDF = guitaristDF.join(bandsDF, joinCondition, "inner")
  //  guitaristBandsDF.show()
  //  guitaristDF.join(bandsDF, joinCondition, "left_outer").show()
  //  guitaristDF.join(bandsDF, joinCondition, "right_outer").show()
  //  guitaristDF.join(bandsDF, joinCondition, "outer").show()
  //  guitaristDF.join(bandsDF, joinCondition, "left_semi").show()
  //  guitaristDF.join(bandsDF, joinCondition, "left_anti").show()

  //  guitaristBandsDF.select("").show()

  guitaristBandsDF.drop(bandsDF.col("id")).show

  guitaristDF.join(bandsDF.withColumnRenamed("id", "band"),
    "band")
    //    .select("id", "name")
    .show()

  val bandModDF = bandsDF.withColumnRenamed("id", "bandID")
  guitaristDF.join(bandModDF, guitaristDF.col("band") === bandModDF.col("bandId")).show()

  guitaristDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)")).show()


}
