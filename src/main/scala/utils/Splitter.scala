package utils

object SplitComma {
  def unapply(s: String):Option[Seq[String]] = Some(s split ",")
}

object SplitColon {
  def unapply(s: String):Option[Seq[String]] = Some(s split ":")
}

object SplitVertical {
  def unapply(s: String):Option[Seq[String]] = Some(s split "\\|")
}
