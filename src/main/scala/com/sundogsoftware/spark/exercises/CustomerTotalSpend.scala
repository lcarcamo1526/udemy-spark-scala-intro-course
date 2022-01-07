package com.sundogsoftware.spark.exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CustomerTotalSpend {

  def parseLine(line: String): (Int, Double) = {
    val parsed = line.split("\\W+")
    (parsed(0).toInt, parsed(2).toDouble)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "CustomerTotalSpend")

    val lines = sc.textFile("data/customer-orders.csv")

    val orders = lines.map(parseLine)

    val totals = orders
      // Sum money spent by customer
      .reduceByKey((spent1, spent2) => spent1 + spent2)
      // Reverse order to take advantage of sortByKey (sort by highest amounts ordered
//      .map(order_total => (order_total._2, order_total._1))
      .sortBy(total => total._2, ascending = false)
      .collect()

    for (total <- totals) println(s"Customer ${total._1} spent ${total._2} in the store")
  }

}
