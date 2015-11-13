import datr.{JsonTest, JsonDetail, MarketLog, NetLog}
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

  it should "parse to None incorrect" in {
    JsonDetail.unapply("{[]}aseasdasd") should be (None)
  }

  it should "parse array of obj" in {
    JsonTest.unapply("""[{"gds":"yo1"}]""") shouldNot be (None)
  }

  "Test" should "parse" in {
    val data = """"[{""gds"":""sabre_ru"",""validatingCarrier"":""QR"",""segments"":[{""departureTime"":""2016-02-01T18:50"",""classOfService"":""Economy"",""route"":""DME>DOH"",""operatingCarrier"":""QR"",""flightNumber"":230,""marketingCarrier"":""QR"",""bookingClass"":""O""},{""departureTime"":""2016-02-02T02:15"",""classOfService"":""Economy"",""route"":""DOH>SIN"",""operatingCarrier"":""QR"",""flightNumber"":944,""marketingCarrier"":""QR"",""bookingClass"":""O""},{""departureTime"":""2016-02-14T10:45"",""classOfService"":""Economy"",""route"":""SIN>DOH"",""operatingCarrier"":""QR"",""flightNumber"":943,""marketingCarrier"":""QR"",""bookingClass"":""O""},{""departureTime"":""2016-02-15T12:30"",""classOfService"":""Economy"",""route"":""DOH>DME"",""operatingCarrier"":""QR"",""flightNumber"":229,""marketingCarrier"":""QR"",""bookingClass"":""O""}],""passengers"":[{""ptc"":""ADT"",""fare"":""8135.00 RUB"",""total"":""27783.00 RUB"",""coupons"":[{""fareBasis"":""OJGSP4PX""},{""fareBasis"":""OJGSP4PX""},{""fareBasis"":""OJGSP4PX""},{""fareBasis"":""OJGSP4PX""}]},{""ptc"":""ADT"",""fare"":""8135.00 RUB"",""total"":""27783.00 RUB"",""coupons"":[{""fareBasis"":""OJGSP4PX""},{""fareBasis"":""OJGSP4PX""},{""fareBasis"":""OJGSP4PX""},{""fareBasis"":""OJGSP4PX""}]},{""ptc"":""ADT"",""fare"":""8135.00 RUB"",""total"":""27783.00 RUB"",""coupons"":[{""fareBasis"":""OJGSP4PX""},{""fareBasis"":""OJGSP4PX""},{""fareBasis"":""OJGSP4PX""},{""fareBasis"":""OJGSP4PX""}]},{""ptc"":""ADT"",""fare"":""8135.00 RUB"",""total"":""27783.00 RUB"",""coupons"":[{""fareBasis"":""OJGSP4PX""},{""fareBasis"":""OJGSP4PX""},{""fareBasis"":""OJGSP4PX""},{""fareBasis"":""OJGSP4PX""}]}]}]""""
    JsonDetail.unapply(data) shouldNot be (None)
  }
}