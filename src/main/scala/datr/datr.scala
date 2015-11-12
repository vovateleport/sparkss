package datr

import com.github.nscala_time.time.Imports._
import utils._

case class MarketLog(
  channel: String,
  query: Query,
  flights: Seq[String],
  agents: Map[String,Price])

object MarketLog{
  def parse(s:String):Option[MarketLog] = {
    //val t=s split ","; println(t,t.size)
    s split "," match {
      case Array(channel, Query(q), SplitVertical(flights), SplitColon(agents), SplitColon(prices)) if agents.size==prices.size
        => Some(MarketLog(channel, q, flights, agents.zip(prices.map(Price(_))).toMap))
      //case Array(channel,q,SplitVertical(flights), SplitColon(agents), SplitColon(prices)) //if agents.size==prices.size
      //  => {println(channel,q);None}
      case _ => None
    }
  }
}

case class Price(amount:Int)

object Price {
  def apply(s:String):Price = {
    val r="""(\d+)\.\d\d RUB""".r
    s match {
      case r(amount) => Price(amount.toInt)
      case _ => ??? //only RUB support
    }
  }
}

case class Query (
  date1: DateTime,
  date2: Option[DateTime],
  from: String,
  to: String,
  countAdults:Int,
  countChilds:Int,
  countInfants:Int,
  clientClass:String)

object Query {
  def unapply(s:String):Option[Query] = {
    val q = """^(\w{7})(\w{3})(\w{3})(\w{7})?AD(\d+)CH(\d+)IN(\d+)(\w{3})$""".r
    s match {
        case q(Date(d1),from,to,Date(d2),ad,ch,in,cc) => Some(Query(d1,Some(d2),from,to,ad.toInt,ch.toInt,in.toInt,cc))
        case q(Date(d1),from,to,_,ad,ch,in,cc) => Some(Query(d1,None,from,to,ad.toInt,ch.toInt,in.toInt,cc))
        //case q(d1,from,to,d2,ad,ch,in,cc) => println("q4",d1,d2);None
        case _ => None
    }
  }
}

object Date {
  def unapply(s:String):Option[DateTime] = {
    val re = """^(\d{2})(\w{3})(\d{2})$""".r
    s match {
      case re(day, Month(m), year) => Some(new DateTime(2000+year.toInt,m,day.toInt,0,0,0,DateTimeZone.forOffsetHours(3)))
      case _ => None
    }
  }
}

object Month {
  def unapply(s:String):Option[Int] = {
    val m:Map[String,Int] = Map(
      "JAN"->1, "FEB"->2, "MAR"->3,
      "APR"->4, "MAY"->5, "JUN"->6,
      "JUL"->7, "AUG"->8, "SEP"->9,
      "OCT"->10, "NOV"->11, "DEC"->12)

    if (m.get(s).isEmpty) println("Month.empty", s)

    m.get(s)
  }
}