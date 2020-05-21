package mycodegen

import java.io.File
import java.nio.file.Files

import utils.ConfigJsonSupport.StreamletsInfoSupport

import spray.json._

import scala.meta._
import scala.io.Source


object Main {

  import utils.ConfigJsonSupport.MyJsonProtocol._

  val fileContents = Source.fromFile("src/main/resources/streamlets_info.json").getLines.mkString
  val streamletsInfoSupports = JsonParser(fileContents).convertTo[StreamletsInfoSupport](streamletsInfoSupport)

  def main(args: Array[String]): Unit = {
    args.toList match {
      case path :: Nil if path.endsWith(".scala") =>
        val jfile = new File(path)
        jfile.getParentFile.mkdirs()
        // Do scala.meta code generation here.
        val str = streamletsInfoSupports.streamlets.foldLeft(
          """
            |package applications.$name$
            |import applications._
            |import cloudflow.spark.sql.SQLImplicits._
            |""".stripMargin)((total, next) => {
          total + source"""\${baseClassString(next.name, next.absClass, next.ports).parse[Stat].get}""".syntax + "\n"
        })
        Files.write(
          jfile.toPath,
          str.getBytes("UTF-8")
        )
    }
  }

  def baseClassString(name: String, absClass: String, ports: List[String]) = {
    s"class \$name extends \$absClass[\${
      ports.foldLeft("")((total, next) => {
        total + next + ","
      }).dropRight(1)
    }]"
  }
}