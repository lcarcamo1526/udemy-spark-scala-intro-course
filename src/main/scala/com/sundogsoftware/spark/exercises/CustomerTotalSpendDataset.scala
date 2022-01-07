package com.sundogsoftware.spark.exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StructType}
import org.apache.spark.sql.functions.{round, sum}

object CustomerTotalSpendDataset {

  case class Order(customer_id: Int, order_id: Int, amount: Float)

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("CustomerOrders")
      .master("local[*]")
      .getOrCreate()


    val orderSchema = new StructType()
      .add("customer_id", IntegerType, nullable = true)
      .add("order_id", IntegerType, nullable = true)
      .add("amount", FloatType, nullable = true)

    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    val orders = spark.read
      .schema(orderSchema)
      .csv("data/customer-orders.csv")
      .as[Order]

    val orders_by_customer = orders.select("customer_id", "amount").groupBy("customer_id")

    val sorted_sums = orders_by_customer.agg(round(sum("amount"), 2).alias("total_spent"))
      .sort("total_spent")

    val results = sorted_sums.collect()

    for {
      result <- results
      customer = result(0)
      total = result(1)
    } println(s"$customer spent $total")

    spark.stop()
  }
}
