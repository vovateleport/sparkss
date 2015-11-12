import datr.{Query, MarketLog, NetLog}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.github.nscala_time.time.OrderingImplicits.DateTimeOrdering
import org.joda.time.DateTime

object App {
  def main(args: Array[String]) {
    val c = new SparkConf()
      .set("spark.task.maxFailures","1")

    val sc = new SparkContext("spark://ip-172-31-22-13:7077","spark1",c)

    try {
      val baseNet:RDD[String] = sc.textFile("/home/ubuntu/data/net_prices-flat_100.csv")
      val baseMarket:RDD[String] = sc.textFile("/home/ubuntu/data/market_prices-flat_100.csv")

      val nc = baseNet.map(NetLog.parse).filter(_.nonEmpty).map(_.get.query)
      val mc = baseMarket.map(MarketLog.parse).filter(_.nonEmpty).map(_.get.query)

      println("count",countMinMax(nc),countMinMax(mc))

    }finally {
      sc.stop()
    }
  }

  def countMinMax(dd:RDD[Query]):(DateTime, Option[DateTime]) = {
    dd.map(q=>(q.date1,q.date2))
      .reduce((p1,p2)=> (
        List(p1._1,p2._1).min,
        List[Option[DateTime]](p1._2, p2._2).flatten match {
          case Nil=> None
          case l => Some(l.max)
        }
      ))
  }
}
