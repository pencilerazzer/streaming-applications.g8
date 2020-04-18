package utils

import spray.json._
object ConfigJsonSupport {

  case class StreamletsInfoSupport(streamlets: List[StreamletInfoSupport])
  case class StreamletInfoSupport(name: String, absClass: String, ports: List[String])

  object MyJsonProtocol extends DefaultJsonProtocol{

    implicit val streamletInfoFormat = jsonFormat3(StreamletInfoSupport.apply)

    implicit val streamletsInfoSupport = jsonFormat1(StreamletsInfoSupport.apply)
  }
}

