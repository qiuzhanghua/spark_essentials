package com.example

import org.apache.spark.sql.SparkSession

object Playground extends App {

  println("Hello, scala")

  val spark = SparkSession.builder()
    .appName("Spark Essentials")
    .config("spark.master", "spark://localhost:7077")
    .getOrCreate()

  val sc = spark.sparkContext
  println("Begin to close ...")

 spark.close()
}
