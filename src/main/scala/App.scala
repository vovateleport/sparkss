import datr.{Query, MarketLog, NetLog}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.github.nscala_time.time.Imports._
import com.github.nscala_time.time.OrderingImplicits.DateTimeOrdering

object App {
  def main(args: Array[String]) {
    val suff = args match { case Array("100") => "_100.csv" case _=>".csv.gz" }

    val c = new SparkConf()
      .set("spark.task.maxFailures","1")

    val sc = new SparkContext("spark://ip-172-31-22-13:7077","spark1",c)

    try {
      val baseNet:RDD[String] = sc.textFile(s"/home/ubuntu/data/net_prices-flat$suff").repartition(sc.defaultParallelism*3)
      //val baseMarket:RDD[String] = sc.textFile(s"/home/ubuntu/data/market_prices-flat$suff").repartition(sc.defaultParallelism*3)
      //println("rows count net,market", baseNet.count(), baseMarket.count())

      val nc = baseNet.map(NetLog.parse).filter(_.nonEmpty).map(_.get)
      println("parsed logs count net", nc.count())//, mc.count())

      println("net gds", listGds(nc))

    }finally {
      sc.stop()
    }
  }

  /**
    * список gds'ов
   */
  def listGds(nc: RDD[NetLog]):Seq[String] = {
    nc.flatMap(nl=>nl.details.map(_.gds)).distinct().collect()
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
