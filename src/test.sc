import com.google.common.collect.ImmutableMap
import com.google.gson._
import scala.collection.JavaConversions._

println(scala.collection.JavaConversions.mapAsScalaMap())

var json: String = "{\"itemScores\":[{\"item\":\"37064\",\"score\":0.0},{\"item\":\"1365\",\"score\":0.0}]}"
var jsonObject = new Gson().fromJson(json, classOf[JsonObject])
println(jsonObject) //{"itemScores":[{"item":"37064","score":0.0},{"item":"1365","score":0.0}]}
println(jsonObject.getClass) //class com.google.gson.JsonObject

val map: Map[String, JsonElement] = jsonObject.asMap().asScala
var mapConvert: Map[String, Object] = Map[String, Object]() //Convert from Json Object to Scala Map

var jsonArray = jsonObject.get("itemScores")
println(jsonArray) //[{"item":"37064","score":0.0},{"item":"1365","score":0.0}]
println(jsonArray.getClass) //class com.google.gson.JsonArray
val list: Seq[JsonElement] = jsonArray.asList().asScala

//var mapList: Seq[Any] = Seq(Map("x" -> 1), Map("x" -> 2))
//println(mapList)
//for (i <- mapList) {
//  println(i.get("x"))
//}


var mapUpdate: scala.collection.mutable.Map[String, Seq[String]] = scala.collection.mutable.Map.empty


