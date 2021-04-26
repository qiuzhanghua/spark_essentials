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

  val carsDF = spark.read.option("inferSchema", "true")
    .schema(carSchema)
    .json(s"src/main/resources/data/cars.json")
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

  case class Guitar(id: Long, make: String, model: String, guitarType: String)

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitars = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json").as[Guitar]
  //  guitars.show()

  val guitarPlayers = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json").as[GuitarPlayer]
  //  guitarPlayers.show()

  val bands = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/bands.json").as[Band]
  //  bands.show()

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayers.joinWith(bands,
    guitarPlayers.col("band") === bands.col("id"),
    "inner"
  )
  //  guitarPlayerBandsDS.show()

  guitarPlayers.joinWith(guitars,
    array_contains(guitarPlayers.col("guitars"), guitars.col("id")),
    "outer"
  )
  //    .show()

  val carsGroupByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
 //   carsGroupByOrigin.show()
}
