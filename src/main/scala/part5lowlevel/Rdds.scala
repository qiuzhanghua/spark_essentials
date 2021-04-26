package com.example
package part5lowlevel

import org.apache.spark.sql.SparkSession

import scala.io.Source

object Rdds extends App {
  val spark = SparkSession.builder()
    .appName("RDDs")
    .config("spark.master", "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val sc = spark.sparkContext
  val numbers = sc.parallelize(1 to 1000000)

  case class StockValue(symbol: String, date: String, price: Double)

  val stocksRDD = sc.parallelize(seq = Source.fromFile("src/main/resources/data/stocks.csv").getLines()
    .drop(1)
    .map(line => line.split(","))
    .map(t => StockValue(t(0), t(1), t(2).toDouble))
    .toList)
  println(stocksRDD.getNumPartitions)
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(t => t(0).toUpperCase() == t(0))
    .map(t => StockValue(t(0), t(1), t(2).toDouble))

  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._

  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  val numbersDF = numbers.toDF("numbers")
  spark.createDataset(numbers)
  numbers.toDS()

  stocksRDD.filter(s => s.symbol == "MSFT")
    .count()

  val companyNamesRDD = stocksRDD.map(_.symbol)
    .distinct()

  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan((a, b) => a.price < b.price)
  val minMSFT = stocksRDD.filter(s => s.symbol == "MSFT").min()
  println(minMSFT)

  numbers.reduce(_ + _)

  val groupStocksRDD = stocksRDD.groupBy(_.symbol)
  // ^^very expensive

  //  val repartitionedStocksRDD = stocksRDD.repartition(32)
  //  repartitionedStocksRDD.toDF().write
  //    .mode(SaveMode.Overwrite)
  //    .parquet("src/main/resources/data/stocks32")
  //  partitions 10MB - 100MB
  //  val coalescedRDD = repartitionedStocksRDD.coalesce(15)
}
