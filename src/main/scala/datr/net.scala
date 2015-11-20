package datr

import utils.SplitVertical

case class NetLog(
                   from: String,
                   to: String,
                   query: Query,
                   flights: Seq[String],
                   details: List[Detail])

object NetLog {
  def parse(s: String): Option[NetLog] = {
    s.split(",|\\>", 5) match {
      case Array(from, to, Query(q), SplitVertical(flights), JsonDetail(details)) => Some(NetLog(from, to, q, flights, details))
      //case Array(from,to,Query(q), SplitVertical(flights),detailString) => println("NetLog_parse", detailString); None
      case _ => None
    }
  }
}

case class Detail(
  gds: String,
  validatingCarrier: String,
  segments:List[Segment],
  passengers:List[Passenger])

case class Segment (
  departureTime:String,
  classOfService: String,
  route: String,
  operatingCarrier: String,
  flightNumber: Int,
  marketingCarrier: String,
  bookingClass: String)

case class Passenger (
  ptc: String,
  fare: String,//"8135.00 RUB"
  total: String,//"27783.00 RUB",
  coupons: List[Coupon])

case class Coupon(fareBasis:String)

case class Test(gds:String)

import spray.json._

object JsonProtocol extends DefaultJsonProtocol {
  implicit val testFormat = jsonFormat1(Test)
  implicit val coupon = jsonFormat1(Coupon)
  implicit val passenger = jsonFormat4(Passenger)
  implicit val segment = jsonFormat7(Segment)
  implicit val detail = jsonFormat4(Detail)
}

import JsonProtocol._

object JsonDetail {
  def unapply(s:String):Option[List[Detail]] = {
    val prepared = s.init.tail.replace("\"\"","\"")

    try {
      val json = prepared.parseJson

      val b = json.convertTo[List[Detail]]

      b match {
        case Nil => None
        case _ => Some(b)
      }
    } catch {
      case e:Exception => None
    }
  }

  def unapply_old(s:String):Option[Detail] = {
    val prepared = s.init.init.tail.tail.replace("\"\"","\"")

    try {
      val json = prepared.parseJson
      //println("JsonDetail", prepared)
      //println(json)

      val b = json.convertTo[Detail]
      //println("NetDetails", b)
      Some(b)
    } catch {
      case e:Exception => None
    }
  }
}

object JsonTest {
  def unapply(s:String):Option[List[Test]] = {
    val json = s.parseJson
    println(json)

    val b = json.convertTo[List[Test]]
    println("JsonTest", b)
    Some(b)
  }
}