package com.predictionIO.evaluateModel

import org.apache.predictionio.sdk.java._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, _}
import org.joda.time._
import com.google.common.collect.ImmutableMap
import com.google.gson.{Gson, JsonObject}

import scala.collection.JavaConverters._


object evaluatePredictionio {
  def main(args: Array[String]): Unit = {
    var engineClient: EngineClient = new EngineClient("http://localhost:8000");

    val query = Map[String, Object]("item" -> "15221", "num" -> "4").asJava
    var response: JsonObject = engineClient.sendQuery(query)

    println(response) //{"itemScores":[{"item":"37064","score":0.0},{"item":"1365","score":0.0},{"item":"1080","score":0.0},{"item":"1278","sco
    println(response.getClass) //class com.google.gson.JsonObject
    var mapConvert: Map[String, Object] = Map[String, Object]() //Convert from Json Object to Scala Map

    var list1 = response.get("itemScores")
    println(list1) //[{"item":"37064","score":0.0},{"item":"1365","score":0.0},{"item":"1080","score":0.0},{"item":
    println(list1.getClass) //class com.google.gson.JsonArray

    var listConvert: Seq[Any] = Seq[Any]() //Convert from Json Array to Scala Seq

//    for (i <- list1) { //Iterate through Json Array directly
//      print(i)




  }
}
