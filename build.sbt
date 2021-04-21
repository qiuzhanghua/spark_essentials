name := "spark_essentials"

version := "0.1"

scalaVersion := "2.12.13"

idePackagePrefix := Some("com.example")

javacOptions ++= Seq("-source", "1.8")
javacOptions ++= Seq("-target", "1.8")

val sparkVersion = "3.0.2"
val postgresVersion = "42.2.19"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.14.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.14.1",
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion
)