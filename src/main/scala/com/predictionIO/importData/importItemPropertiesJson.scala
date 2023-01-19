package com.predictionIO.importData

import org.apache.predictionio.sdk.java.{Event, EventClient, FileExporter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, _}
import org.joda.time._

object importItemPropertiesJson {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.getOrCreate()

    def importItemProperties(): Unit = {
      var sqlItemProperties =
        """
          |with cte as(
          |select c.content_id, collect_set(aud.author_name) over(partition by c.content_id order by aud.author_name) as author_name,
          |collect_set(cad.category_name) over(partition by c.content_id order by cad.category_name) as category_name
          |    from waka.content_dim as c join waka.content_author_brid as au on c.content_id = au.content_id
          |    join (select * from waka.author_dim where cast(author_name as integer) is null) as aud on au.author_id = aud.author_id
          |    join waka.content_category_brid as ca on c.content_id = ca.content_id join waka.category_dim as cad on ca.category_id = cad.category_id
          |),
          |cte1 as(
          |select content_id, author_name, category_name, row_number() over(partition by content_id order by size(category_name) desc) as rank from cte
          |)
          |select content_id, author_name, category_name from cte1 where rank = 1
          |""".stripMargin
      val dfItemProperities = spark.sql(sqlItemProperties)
//      var EventSchema = StructType(Array(
//        StructField("event",StringType),
//        StructField("entityType",StringType),
//        StructField("entityId",StringType),
//        StructField("targetEntityType",StringType),
//        StructField("targetEntityId",StringType),
//        StructField("properties",MapType(StringType, StringType)),
//        StructField("eventTime",DateType)
//        )
//      )
      val colToMap = List("author_name", "category_name") // Turn 2 columns into a Map
        .flatMap(colName => List(lit(colName), col(colName)))

      val itemPropertiesEventJson = dfItemProperities
        .withColumn("event", lit("$set"))
        .withColumn("entityType", lit("item"))
        .withColumn("entityId", col("content_id").cast(StringType))
        .withColumn("properties", map(colToMap: _ *)) //sql map func (key1, val1, key2, val2,...) all must be column. _* mean extract all elems
        .withColumn("eventTime", lit(current_timestamp()))
        .select("event", "entityType", "entityId", "properties",  "eventTime")

      itemPropertiesEventJson.write.json("propertiesEvent.json")
      // Luu trong hdfs hdfs://vftsandbox-namenode:8020/user/vgdata/propertiesEvent.json
      // hadoop fs -cat /user/vgdata/propertiesEvent.json/* | hadoop fs -put - /user/vgdata/mergedPropertiesEvent.json
      // hadoop fs -get /user/vgdata/mergedPropertiesEvent.json /home/vgdata/universal
      // pio import --appid 3 --input /home/vgdata/universal/mergedPropertiesEvent.json
    }

    importItemProperties()
    spark.stop()
  }

}
