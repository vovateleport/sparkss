package datr

case class PricesBag (
  query:Query,
  market:Market
                     )

case class Market (
                       channel: String,
                       flights: Seq[String],
                       agents: Map[String,Price]
                     )

