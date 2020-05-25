package utils.Parameters

object CastMode {
  def apply(modeS: String): CastMode = modeS.toLowerCase match {
    case "permissive"    => PERMISSIVE
    case "notpermissive" => NOTPERMISSIVE
    case _               => throw new RuntimeException(s"Cast mode $modeS not allowed")
  }
}

sealed trait CastMode

case object PERMISSIVE extends CastMode

case object NOTPERMISSIVE extends CastMode