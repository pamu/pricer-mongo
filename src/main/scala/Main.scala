import java.io.{FilenameFilter, File, PrintWriter}

import com.mongodb.casbah.MongoClient
import com.mongodb.util.JSON
import play.api.libs.json.{JsValue, JsPath, Writes, Json}
import play.api.libs.functional.syntax._
import scala.collection.mutable
import scala.io.Source

import com.mongodb.casbah.TypeImports.DBObject

/**
  * Created by pnagarjuna on 06/11/15.
  */
object Main {
  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println(s"Please give data folder path")
      sys.exit()
    }

    println(s"Command line argument ${args(0)}")

    val dataFolder = new File(args(0))

    if (! dataFolder.exists()) {
      println(s"Folder doesn't exist")
      sys.exit()
    } else {
      println(s"Data Folder exists")
    }


    val folders = dataFolder.listFiles().toList

    val maps = getMaps
    val a = maps._1
    val b = maps._2

    dumpToMongo(Json.toJson(a).toString(), Json.toJson(b).toString(), Json.toJson(cities.map(_.swap)).toString())



    folders.foreach { folder =>
      if (folder.isDirectory) {
        val fileList = folder.listFiles(new FilenameFilter {
          override def accept(dir: File, name: String): Boolean = {
            name.contains(".csv")
          }
        }).toList
        println(s"processing ${folder.getName} data")
        fileList.foreach { file =>
          if (file.isFile) {
            println(s"processing ${file.getName} file")
            process(file)
          }
        }
      } else {
        println(s"ignoring an item because its a file named ${folder.getPath}")
      }
    }
   }

  def process(filename: File): Unit = {
    replaceString(filename, keyValues(filename), cities)
    dumpPricerData(filename)
  }

  def cities = Map(
    "Mumbai" -> 1,
    "Bangalore" -> 2,
    "Chennai" -> 3,
    "Delhi" -> 4,
    "Pune" -> 5
  )

  implicit val cityWrites: Writes[Map[Int, String]] = new Writes[Map[Int, String]] {
    override def writes(o: Map[Int, String]): JsValue = {
      Json.obj(
        "list" ->
          Json.toJson(
            o.map { pair =>
              Json.obj("id" -> pair._1, "name" -> pair._2)
            }
          )
      )
    }
  }
  implicit val makeWrites: Writes[Make] = (
    (JsPath \ "name").write[String] and
      (JsPath \ "id").write[Int]
    ) (unlift(Make.unapply _))

  implicit val modelWrites: Writes[Model] = (
    (JsPath \ "name").write[String] and
      (JsPath \ "id").write[Int]
    ) (unlift(Model.unapply _))

  implicit val versionWrites: Writes[Version] = (
    (JsPath \ "name").write[String] and
      (JsPath \ "id").write[Int]
    ) (unlift(Version.unapply _))

  implicit val mapMakesWrites: Writes[scala.collection.mutable.Map[Make, Set[Model]]] = new Writes[mutable.Map[Make, Set[Model]]] {
    override def writes(o: mutable.Map[Make, Set[Model]]): JsValue = {
      Json.obj(
        "list" ->
          Json.toJson {
            o.map { pair =>
              Json.obj("make" -> Json.toJson(pair._1), "models" -> Json.toJson(pair._2))
            }.toList
          }
      )
    }
  }

  implicit val mapModelsWrites: Writes[scala.collection.mutable.Map[Model, Set[Version]]] = new Writes[mutable.Map[Model, Set[Version]]] {
    override def writes(o: mutable.Map[Model, Set[Version]]): JsValue = {
      Json.obj(
        "list" -> Json.toJson {
          o.map { pair =>
            Json.obj("model" -> Json.toJson(pair._1), "versions" -> Json.toJson(pair._2))
          }
        }
      )
    }
  }

  case class Make(name: String, id: Int)

  case class Model(name: String, id: Int)

  case class Version(name: String, id: Int)

  def getMaps: (scala.collection.mutable.Map[Make, Set[Model]], scala.collection.mutable.Map[Model, Set[Version]]) = {

    val lines = Source.fromInputStream(getClass.getResourceAsStream("make-model-version.csv"), "UTF-8").getLines()

    val makeModels = scala.collection.mutable.Map[Make, Set[Model]]()
    val modelVersions = scala.collection.mutable.Map[Model, Set[Version]]()

    def processLine(line: String): Unit = {
      val dust = line.split("    ").map(_.trim)
      val makes = dust(1)
      val mDust = makes.split(",").map(_.trim)
      val make = mDust(0)
      val makeId = mDust(1)
      val models = dust(2)
      val moDust = models.split(",").map(_.trim)
      val model = moDust(0)
      val modelId = moDust(1)
      val versions = dust(3)
      val vDust = versions.split(",").map(_.trim)
      val version = vDust(0)
      val versionId = vDust(1)

      val cMake = Make(make, makeId.toInt)
      val cModel = Model(model, modelId.toInt)
      val cVersion = Version(version, versionId.toInt)

      if (makeModels contains cMake) {
        makeModels(cMake) = makeModels(cMake) + cModel
      } else {
        makeModels += (cMake -> Set[Model](cModel))
      }

      if (modelVersions contains cModel) {
        modelVersions(cModel) = modelVersions(cModel) + cVersion
      } else {
        modelVersions += (cModel -> Set[Version](cVersion))
      }

    }

    lines.foreach(processLine(_))

    (makeModels -> modelVersions)
  }

  def keyValues(filename: File): scala.collection.mutable.Map[String, Int] = {
    val lines = Source.fromInputStream(getClass.getResourceAsStream("make-model-version.csv"), "UTF-8").getLines()

    val map = scala.collection.mutable.Map[String, Int]()

    def processLine(line: String): Unit = {
      val dust = line.split("    ").map(_.trim)
      val makes = dust(1)
      val mDust = makes.split(",").map(_.trim)
      val make = mDust(0)
      val makeId = mDust(1)
      val models = dust(2)
      val moDust = models.split(",").map(_.trim)
      val model = moDust(0)
      val modelId = moDust(1)
      val versions = dust(3)
      val vDust = versions.split(",").map(_.trim)
      val version = vDust(0)
      val versionId = vDust(1)

      if (model == "Land Cruiser") {
        map += (make -> makeId.toInt)
        map += ("Land Cruiser [2011-2015]" -> modelId.toInt)
        map += (version -> versionId.toInt)
      } else {
        map += (make -> makeId.toInt)
        map += (model -> modelId.toInt)
        map += (version -> versionId.toInt)
      }
    }

    lines.foreach(processLine(_))
    map
  }

  def getTempFile(filename: File): File = {
    val folder = filename.getParentFile
    new File(folder, folder.getName + "_processed.csv")
  }

  def getIdsFile(filename: File): File = {
    val folder = filename.getParentFile
    new File(folder, folder.getName + "_processed-ids.csv")
  }

  def replaceString(filename: File, map: scala.collection.mutable.Map[String, Int], cities: Map[String, Int]): Unit = {
    val lines = Source.fromFile(filename, "UTF-8").getLines()
    val file = getTempFile(filename)
    val writer = new PrintWriter(file)
    lines.foreach { line =>
      val newLine = createNewLine(line, map, cities)
      writer.println(newLine)
      writer.flush()
    }
    writer.close()
  }

  def createNewLine(line: String, map: scala.collection.mutable.Map[String, Int], cities: Map[String, Int]): String = {
    val cols = line.split("    ").map(_.trim)
    val year = cols(0)
    val make = cols(1)
    val model = cols(2)
    val version = cols(3)
    val city = cols(4)
    val month = cols(5)
    val kms = cols(6)
    val fair = cols(7)
    val good = cols(8)
    val excellent = cols(9)

    println(s"$year $make $model $version $city $month $kms $fair $good $excellent")

    val fs = "    "

    s"$year$fs${map(make)}$fs${map(model)}$fs${map(version)}$fs${cities(city)}$fs$month$fs$kms$fs$fair$fs$good$fs$excellent"
  }

  def dumpToMongo(makesModels: String, modelsVersions: String, cities: String): Unit = {
    val mongoClient = MongoClient()

    val zoomo = mongoClient("zoomo")

    (Json.parse(makesModels) \ "list").as[List[JsValue]].map { json =>
      zoomo("pricer_make_model").insert(JSON.parse(json.toString).asInstanceOf[DBObject])
    }
    (Json.parse(modelsVersions) \ "list").as[List[JsValue]].map { json =>
      zoomo("pricer_model_version").insert(JSON.parse(json.toString).asInstanceOf[DBObject])
    }
    (Json.parse(modelsVersions) \ "list").as[List[JsValue]].map { json =>
      zoomo("pricer_cities").insert(JSON.parse(json.toString).asInstanceOf[DBObject])
    }

    mongoClient.close()

  }

  def dumpPricerData(filename: File): Unit  = {
    val lines = Source.fromFile(getTempFile(filename)).getLines()

    val mongoClient = MongoClient()
    val zoomo = mongoClient("zoomo")

    lines.foreach { line =>
      val cols = line.split("    ").map(_.trim)
      val year = cols(0).toInt
      val make = cols(1).toInt
      val model = cols(2).toInt
      val version = cols(3).toInt
      val city = cols(4).toInt
      val month = cols(5)
      val kms = cols(6).toInt
      val fair = cols(7).toInt
      val good = cols(8).toInt
      val excellent = cols(9).toInt

      val data = Json.obj(
        "year" -> year,
        "make" -> make,
        "model" -> model,
        "version" -> version,
        "city" -> city,
        "month" -> month,
        "kms" -> kms,
        "fair_price" -> fair,
        "good_price" -> good,
        "good_excellent" -> excellent
      )
      zoomo("pricer_data").insert(JSON.parse(data.toString).asInstanceOf[DBObject])
    }

    mongoClient.close()
  }

  def pricerData(filename: File): Unit = {
    val lines = Source.fromFile(getTempFile(filename)).getLines()
    lines.foreach { line =>
      val cols = line.split("    ").map(_.trim)
      val year = cols(0).toInt
      val make = cols(1)
      val model = cols(2)
      val version = cols(3).toInt
      val city = cols(4).toInt
      val month = cols(5)
      val kms = cols(6).toInt
      val fair = cols(7).toInt
      val good = cols(8).toInt
      val excellent = cols(9).toInt

      val data = Json.obj(
        "year" -> year,
        "make" -> make,
        "model" -> model,
        "version" -> version,
        "city" -> city,
        "month" -> month,
        "kms" -> kms,
        "fair_price" -> fair,
        "good_price" -> good,
        "excellent_price" -> excellent
      )
    }
  }
}
