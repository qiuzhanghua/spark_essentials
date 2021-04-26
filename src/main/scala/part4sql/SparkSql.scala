package com.example
package part4sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkSql extends App {
  val spark = SparkSession.builder()
    .appName("Spark SQL")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")


  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // equal

  carsDF.createOrReplaceTempView("cars")
  spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin)
    // .show()

  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  spark.sql("show databases")
//    .show()

}

