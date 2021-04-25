package com.example
package typesdataset

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import java.sql.Date

object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Common Type")
    .config("spark.master", "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val numbersDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/numbers.csv")
  //   .show()

  //  numbersDF.printSchema()

  implicit val encoder: Encoder[Int] = Encoders.scalaInt

  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  numbersDS.filter(_ < 100)
  //    .show()

  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: Date,
                  Origin: String
                )

  val carSchema = StructType(Seq(
    StructField("Name", StringType, nullable = false),
    StructField("Miles_per_Gallon", DoubleType, nullable = false),
    StructField("Cylinders", LongType, nullable = false),
    StructField("Displacement", DoubleType, nullable = false),
    StructField("Horsepower", LongType, nullable = false),
    StructField("Weight_in_lbs", LongType, nullable = false),
    StructField("Acceleration", DoubleType, nullable = false),
    StructField("Year", DateType, nullable = false),
    StructField("Origin", StringType, nullable = false),
  ))

  def readDF(filename: String) =
    spark.read.option("inferSchema", "true")
      .schema(carSchema)
      .json(s"src/main/resources/data/$filename.json")

  val carsDF = readDF("cars")
  //  carsDF.show()
  //  implicit val carEncoder = Encoders.product[Car]

  import spark.implicits._

  val carsDS = carsDF.as[Car]
  //  carsDS.show()
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())
  //  carNamesDS.show()
  val carsCount = carsDS.count()
  carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count
  carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount
  carsDS.select(avg(col("Horsepower")))
}
