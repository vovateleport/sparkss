package snow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object App {
  def main(args: Array[String]) {
    val c = new SparkConf().setJars(Seq("file://home/ubuntu/sparkss/target/scala-2.10/sparkss_2.10-1.0.jar"))
    val sc = new SparkContext("spark://ip-172-31-22-13:7077","spark1",c)

    try {
      /*val baseNet:RDD[String] = sc.textFile("C:\\projects\\datravel1\\net_prices-flat_100.csv")
      val baseMarket:RDD[String] = sc.textFile("C:\\projects\\datravel1\\market_prices-flat_100.csv")

      val nc = baseNet.map(NetLog.parse).count()
      val mc = baseMarket.map(MarketLog.parse).count()

      println("count",nc,mc);
      */
      //val base0 = sc.textFile("C:\\projects\\datravel1\\net_prices-flat.csv").take(100)
      //base0.saveAsTextFile("C:\\projects\\datravel1\\net_prices-flat_100.csv")
      //println("YOOO", base0.take(1)(0))
      val num = 1000
      val count = sc.parallelize(1 to num, 1).map{i =>
        val x = Math.random()
        val y = Math.random()
        if (x*x + y*y < 1) 1 else 0
      }.reduce(_ + _)
      println("Pi is roughly " + 4.0 * count / num)
    }finally {
      sc.stop()
    }
  }
}
