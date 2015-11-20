import datr.{Query, MarketLog, NetLog}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._
import com.github.nscala_time.time.Imports._
import com.github.nscala_time.time.OrderingImplicits.DateTimeOrdering

object tasks {
  def agentAmount(mc: RDD[MarketLog]) = {
    val tuples = mc.flatMap(ml=>ml.agents.map{case (ag, p)=>(ag,1)}.toList)
      .reduceByKey(_+_)
      .collect()

    println(tuples.sortBy{case (ag,c)=>(-c)}.mkString("\n"))
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