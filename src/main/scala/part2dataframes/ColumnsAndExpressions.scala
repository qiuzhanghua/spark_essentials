package com.example
package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")
  carsDF.show()

  val firstColumn = carsDF.col("Name")

  val carNamesDF = carsDF.select(firstColumn)
  carNamesDF.show()

  import spark.implicits._

  carsDF.select(
    col("Name"),
    column("Acceleration"),
    'Year,
    $"Horsepower",
    expr("Origin")
  ).show()

  carsDF.select("Name", "Year").show()

  val simplestExpression = carsDF.col("weight_in_lbs")
  val weightInKgExpression = carsDF.col("weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("name"),
    col("weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("weight_in_lbs / 2.2").as("weight2")
  )
  carsWithWeightsDF.show()
  carsDF.selectExpr(
    "Name",
    "weight_in_lbs",
    "weight_in_lbs / 2.2 as weight3"
  ).show()

  // add new column
  carsDF.withColumn("weight_in_kg_3", col("weight_in_lbs") / 2.2).show()
  // carsDF.withColumnRenamed("weight_in_lbs", "weight in lbs").selectExpr("`weight in lbs`")
  //carsDF.drop()

  carsDF.filter(col("Origin") =!= "USA").show()
  // carsDF.where(col("Origin") =!= "USA").show()
  carsDF
    .filter(col("Origin") === "USA")
    .filter(col("horsepower") > 150)
    .show()
  //  carsDF
  //    .filter((col("Origin") === "USA")
  //      .and(col("horsepower") > 150))
  //    .show()
  //    carsDF.filter("Origin = 'USA' and Horsepower > 150").show()

  //  spark.read
  //    .option("inferSchema", "true")
  //    .json("src/main/resources/data/more_cars.json")
  //    .union(carsDF).show()
  carsDF.select("Origin").distinct().show()
}
