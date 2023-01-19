package com.test

import org.apache.spark.sql._

object testSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val data = Seq(("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
      ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
      ("Carrots", 1200, "China"), ("Beans", 1500, "China"))

    //DataFrame
    val df = spark.createDataFrame(data).toDF("Product", "Amount", "Country")
    println(df.getClass)
    df.show()
    var returnVal: Int = 0

    def testFunc(param1: Row, param2: String): Int = {
      println(param1)
      println(param2)
      println(returnVal)
      returnVal += 1
      println(returnVal)
      println("-----")
      return 3
    }
    var param2Var = "abc"

    df.foreach(f => testFunc(f, param2Var))
    println(returnVal)
  }
}
