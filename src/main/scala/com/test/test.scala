package com.test

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.gson.{Gson, JsonObject}
//import scala.collection.mutable.Map


object test {
  def main(args: Array[String]): Unit = {

    var json: String = "{\"itemScores\":[{\"item\":\"37064\",\"score\":0.0},{\"item\":\"1365\",\"score\":0.0}]}"
    var jsonObject = new Gson().fromJson(json, classOf[JsonObject])
    println(jsonObject) //{"itemScores":[{"item":"37064","score":0.0},{"item":"1365","score":0.0}]}
    println(jsonObject.getClass) //class com.google.gson.JsonObject


    var mapConvert: Map[String, Seq[Any]] = Map[String, Seq[Any]]() //Convert from Json Object to Scala Map

    var list1 = jsonObject.get("itemScores")
    println(list1) //[{"item":"37064","score":0.0},{"item":"1365","score":0.0}]
    println(list1.getClass) //class com.google.gson.JsonArray

    var listConvert: Seq[Map[Any, Any]] = Seq[Map[Any, Any]]() //Convert from Json Array to Scala Seq

    //    for (i <- list1) { //Iterate through Json Array directly
    //      print(i)

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapConvert = mapper.readValue(jsonObject.toString, classOf[Map[String, Seq[Any]]])
    listConvert = mapper.readValue(list1.toString, classOf[Seq[Map[Any, Any]]])
    println(mapConvert) //Map(itemScores -> List(Map(item -> 37064, score -> 0.0), Map(item -> 1365, score -> 0.0)))
    println(listConvert) //List(Map(item -> 37064, score -> 0.0), Map(item -> 1365, score -> 0.0))

    var mapList: Seq[String] = listConvert.map(x => {x.get("item").toString})


    var mapUpdate: scala.collection.mutable.Map[String, Seq[String]] = scala.collection.mutable.Map.empty

    for ( i <- 1 to 5) {
      mapUpdate += i.toString() -> mapList
    }
    print(mapUpdate)
  }
}
