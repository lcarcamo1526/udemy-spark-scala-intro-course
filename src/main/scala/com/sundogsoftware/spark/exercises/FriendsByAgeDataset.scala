package com.sundogsoftware.spark.exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FriendsByAgeDataset {

  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]


    people.show()
    people.select("age", "friends")
      .groupBy("age")
      .avg("friends")
      .sort("age").show()

    spark.stop()
  }
}
