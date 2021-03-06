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
    val c = new SparkConf().set("spark.task.maxFailures","1")
    val sc = new SparkContext(urlSpark,"spark1",c)
    try {
      process_load(sc, args)
    }finally {
      sc.stop()
    }
  }

  /**
    * Главная функция для выполнения тасков, куда приходят загруженные RDD
    * */
  def process(nc:RDD[NetLog], mc:RDD[MarketLog]) {
    //println("counts net market", nc.count(),mc.count())
    tasks.agentAmount(mc)
  }

  def process_load(sc: SparkContext, args:Array[String]) {
    val suff = if (args contains "100") "_100.csv" else ".csv.gz"

    args match {
      case Array("convert", _*) =>
        val baseNet: RDD[String] = sc.textFile(pathLog + fileNet + suff)
        val baseMarket: RDD[String] = sc.textFile(pathLog + fileMarket + suff)
        //println("rows count net,market", baseNet.count(), baseMarket.count())
        val nc0 = baseNet.map(NetLog.parse).filter(_.nonEmpty).map(_.get)
        val mc0 = baseMarket.map(MarketLog.parse).filter(_.nonEmpty).map(_.get)

        nc0.saveAsObjectFile(pathConverted + fileNet); println("net saved to", pathConverted + fileNet)
        mc0.saveAsObjectFile(pathConverted + fileMarket); println("marked saved to", pathConverted + fileMarket)
      case Array("plain", _*) =>
        val baseNet: RDD[String] = sc.textFile(pathLog + fileNet + suff).repartition(sc.defaultParallelism * 3)
        val baseMarket: RDD[String] = sc.textFile(pathLog + fileMarket + suff).repartition(sc.defaultParallelism * 3)
        //println("rows count net,market", baseNet.count(), baseMarket.count())
        val nc0 = baseNet.map(NetLog.parse).filter(_.nonEmpty).map(_.get)
        val mc0 = baseMarket.map(MarketLog.parse).filter(_.nonEmpty).map(_.get)
        process(nc0, mc0)
      case _ =>
        val nc0 = sc.objectFile[NetLog](pathConverted + fileNet, sc.defaultParallelism * 3)
        val mc0 = sc.objectFile[MarketLog](pathConverted + fileMarket, sc.defaultParallelism * 3)
        process(nc0, mc0)
    }
  }


}
