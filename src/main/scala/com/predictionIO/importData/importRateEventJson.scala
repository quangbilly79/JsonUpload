package com.predictionIO.importData

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object importRateEventJson {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.getOrCreate()
    def importRateJson(): Unit = {
      var sqlRate =
        """
          |select distinct user_id, content_id, rate from waka.waka_pd_fact_rate
          |where data_date_key < 20220701""".stripMargin
      var dfRate = spark.sql(sqlRate)
      val rateEventJson = dfRate
        .withColumn("event", lit("rate"))
        .withColumn("entityType", lit("user"))
        .withColumn("entityId", col("user_id").cast(StringType))
        .withColumn("targetEntityType", lit("item"))
        .withColumn("targetEntityId", col("content_id").cast(StringType))
        .withColumn("properties", map(lit("rate"), col("rate")))
        .withColumn("eventTime", lit(current_timestamp()))
        .select("event", "entityType", "entityId", "targetEntityType", "targetEntityId", "properties", "eventTime")

      rateEventJson.write.json("rateEvent.json")
    }
    importRateJson()
    spark.stop()
  }
}
// Luu trong hdfs hdfs://vftsandbox-namenode:8020/user/vgdata/rateEvent.json
// hadoop fs -cat /user/vgdata/rateEvent.json/* | hadoop fs -put - /user/vgdata/mergedRateEvent.json
// hadoop fs -get /user/vgdata/mergedRateEvent.json /home/vgdata/universal
// pio import --appid 3 --input /home/vgdata/universal/mergedRateEvent.json