import datr.{MarketLog, NetLog}
import org.scalatest._
import scala.io.Source

class ParserSpec extends FlatSpec with Matchers {

  "Parser" should "parse net prices" in {
    val source = Source.fromURL(getClass.getResource("/net_prices-flat_100.csv"),"utf8")
    val lines = source.getLines().toList
    val logs:Seq[NetLog] = lines.map(NetLog.parse).flatten
    lines.length should be (100)
    logs.length should be (100)
    source.close()
  }

  it should "parse market prices" in {
    val source = Source.fromURL(getClass.getResource("/market_prices-flat_100.csv"),"utf8")
    val lines = source.getLines().toList
    val logs:Seq[MarketLog] = lines.map(MarketLog.parse).flatten
    lines.length should be (100)
    logs.length should be (100)
    source.close()
  }
}