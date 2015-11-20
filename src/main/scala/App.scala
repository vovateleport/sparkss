import datr.{Query, MarketLog, NetLog}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import SparkContext._
import com.github.nscala_time.time.Imports._
import com.github.nscala_time.time.OrderingImplicits.DateTimeOrdering

object App {
  val urlSpark = "spark://ip-172-31-22-13:7077"
  val pathLog = "/home/ubuntu/data/"
  val pathConverted = "/run/shm/"
  val fileNet = "net_prices-flat"
  val fileMarket = "market_prices-flat"


  def main(args: Array[String]) {
    val suff = if (args contains "100") "_100.csv" else ".csv.gz"

    val c = new SparkConf().set("spark.task.maxFailures","1")
    val sc = new SparkContext(urlSpark,"spark1",c)

    try {
      val (nc, mc) = args.contains("convert") match {
        case true =>
          val baseNet:RDD[String] = sc.textFile(pathLog + fileNet + suff).repartition(sc.defaultParallelism*3)
          val baseMarket:RDD[String] = sc.textFile(pathLog + fileMarket + suff).repartition(sc.defaultParallelism*3)
          //println("rows count net,market", baseNet.count(), baseMarket.count())
          val nc0 = baseNet.map(NetLog.parse).filter(_.nonEmpty).map(_.get)
          val mc0 = baseMarket.map(MarketLog.parse).filter(_.nonEmpty).map(_.get)

          nc0.saveAsObjectFile(pathConverted+fileNet); println("net saved to",pathConverted+fileNet)
          mc0.saveAsObjectFile(pathConverted+fileMarket); println("marked saved to",pathConverted+fileMarket)
          (nc0, mc0)
        case false =>
          val baseNet:RDD[String] = sc.textFile(pathLog + fileNet + suff).repartition(sc.defaultParallelism*3)
          val baseMarket:RDD[String] = sc.textFile(pathLog + fileMarket + suff).repartition(sc.defaultParallelism*3)
          //println("rows count net,market", baseNet.count(), baseMarket.count())
          val nc0 = baseNet.map(NetLog.parse).filter(_.nonEmpty).map(_.get)
          val mc0 = baseMarket.map(MarketLog.parse).filter(_.nonEmpty).map(_.get)

          //nc0.saveAsSequenceFile(pathConverted+fileNet)
          //mc0.saveAsSequenceFile(pathConverted+fileMarket)
          (nc0, mc0)
      }

      //val baseNet:RDD[String] = sc.textFile(pathLog + fileNet + suff).repartition(sc.defaultParallelism*3)
      //val baseMarket:RDD[String] = sc.textFile(pathLog + fileMarket + suff).repartition(sc.defaultParallelism*3)

      //println("rows count net,market", baseNet.count(), baseMarket.count())

      //val nc = baseNet.map(NetLog.parse).filter(_.nonEmpty).map(_.get)
      //val mc = baseMarket.map(MarketLog.parse).filter(_.nonEmpty).map(_.get)

      //println("parsed logs count net, market", nc.count(), mc.count())

     println("counts net market", nc.count(),mc.count())

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
