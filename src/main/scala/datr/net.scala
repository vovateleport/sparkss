package datr

import utils.SplitVertical


case class NetLog(
                   from: String,
                   to: String,
                   query: Query,
                   flights: Seq[String],
                   detail: Detail)

object NetLog {
  def parse(s: String): Option[NetLog] = {
    s.split(",|\\>", 5) match {
      case Array(from, to, Query(q), SplitVertical(flights), JsonDetail(detail)) => Some(NetLog(from, to, q, flights, detail))
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
  def unapply(s:String):Option[Detail] = {
    val prepared = s.init.init.tail.tail.replace("\"\"","\"")
    val json = prepared.parseJson
    //println("JsonDetail", prepared)
    //println(json)

    val b = json.convertTo[Detail]
    //println("NetDetails", b)
    Some(b)
  }
}
