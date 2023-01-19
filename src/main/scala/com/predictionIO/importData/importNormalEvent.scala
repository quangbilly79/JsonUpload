package com.predictionIO.importData

import org.apache.predictionio.sdk.java.{Event, EventClient}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import scala.collection.JavaConverters._
object importNormalEvent {
  var client = new EventClient("lpFLJ5o83vW1B0LLGNQ7mOoZxdx43h2dUyAZpsjdkIYwwTDktM42p48gUosasnV7",
    "http://172.25.48.219:7070")


  def parseRate(row: Row) {

    var importedRate = new(Event)
      .event("rate")
      .entityType("user")
      .entityId(row.getAs[Any]("user_id").toString())
      .targetEntityType("item")
      .targetEntityId(row.getAs[Any]("content_id").toString())
      .properties(Map("rate"-> row.getAs[AnyRef]("rate")).asJava)

    client.createEvent(importedRate)
  }

  def parseWishlist(row: Row) {

    var importedWishlist = new(Event)
      .event("wishlist")
      .entityType("user")
      .entityId(row.getAs[Any]("user_id").toString())
      .targetEntityType("item")
      .targetEntityId(row.getAs[Any]("content_id").toString())

    client.createEvent(importedWishlist)
  }



  def parseRead(row: Row) {

    var importedRead = new(Event)
      .event("read")
      .entityType("user")
      .entityId(row.getAs[Any]("user_id").toString())
      .targetEntityType("item")
      .targetEntityId(row.getAs[Any]("content_id").toString())

    client.createEvent(importedRead)
  }

  def main(args: Array[String]) {
    var spark = SparkSession.builder.getOrCreate()
    def importRead(): Unit = {
      var sqlRead =
        """
          |select distinct user_id, content_id from waka.waka_pd_fact_reader
          |where data_date_key < 20220701""".stripMargin
      var dfRead = spark.sql(sqlRead)
      dfRead.foreach(x => parseRead(x))
    }

    def importWishlist(): Unit = {

      var sqlWishlist =
        """
          |select distinct user_id, content_id from waka.waka_pd_fact_wishlist
          |where data_date_key < 20220701""".stripMargin
      var dfWishlist = spark.sql(sqlWishlist)
      dfWishlist.foreach(x => parseWishlist(x))
    }

    def importRate(): Unit = {
      var sqlRate =
        """
          |select distinct user_id, content_id, rate from waka.waka_pd_fact_rate
          |where data_date_key < 20220701""".stripMargin
      var dfRate = spark.sql(sqlRate)
      dfRate.foreach(x => parseRate(x))
    }
    importRate()
    importWishlist()
    importRead()
    spark.stop()
  }

}
