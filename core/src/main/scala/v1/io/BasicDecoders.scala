package ru.itclover.tsp.io

trait BasicDecoders[From] {
  implicit def decodeToDouble: Decoder[From, Double]
  implicit def decodeToFloat: Decoder[From, Float]
  implicit def decodeToInt: Decoder[From, Int]
  implicit def decodeToString: Decoder[From, String]
  implicit def decodeToAny: Decoder[From, Any]
}


// TBD: Implement an error handling strategy with Cats Validated
object AnyDecodersInstances extends BasicDecoders[Any] with Serializable {
  import Decoder._


  implicit val decodeToDouble: Decoder[Any, Double] = new AnyDecoder[Double] {
    override def apply(x: Any) = x match {
      case d: Double           => d
      case n: java.lang.Number => n.doubleValue()
      case s: String =>
        try { Helper.strToDouble(s) } catch {
          case e: Exception => 0.0
            //throw new RuntimeException(s"Cannot parse String ($s) to Double, exception: ${e.toString}")
        }
      case null => 0.0   // TSP-219 fix
      case _    => 0.0   // FIXME
    }
  }
  
  implicit val decodeToFloat: Decoder[Any, Float] = new AnyDecoder[Float] {
    override def apply(x: Any) = x match {
      case d: Float            => d
      case n: java.lang.Number => n.floatValue()
      case s: String =>
        try { Helper.strToFloat(s) } catch {
          case e: Exception =>
            throw new RuntimeException(s"Cannot parse String ($s) to Float, exception: ${e.toString}")
        }
      case null => Float.NaN
    }
  }

  implicit val decodeToInt: Decoder[Any, Int] = new AnyDecoder[Int] {
    override def apply(x: Any) = x match {
      case i: Int              => i
      case n: java.lang.Number => n.intValue()
      case s: String =>
        try { Helper.strToInt(s) } catch {
          case e: Exception => throw new RuntimeException(s"Cannot parse String ($s) to Int, exception: ${e.toString}")
        }
      case null => throw new RuntimeException(s"Cannot parse null to Int")
    }
  }

  implicit val decodeToLong: Decoder[Any, Long] = new AnyDecoder[Long] {
    override def apply(x: Any) = x match {
      case i: Int              => i
      case l: Long             => l
      case n: java.lang.Number => n.longValue()
      case s: String =>
        try { Helper.strToInt(s) } catch {
          case e: Exception => throw new RuntimeException(s"Cannot parse String ($s) to Int, exception: ${e.toString}")
        }
      case null => throw new RuntimeException(s"Cannot parse null to Long")
    }
  }

  implicit val decodeToBoolean: Decoder[Any, Boolean] = new AnyDecoder[Boolean] {
    override def apply(x: Any) = x match {
      case 0 | 0L | 0.0 | "0" | "false" | "off" | "no" => false
      case 1 | 1L | 1.0 | "1" | "true" | "on" | "yes"  => true
      case b: Boolean                                  => b
      case null                                        => throw new RuntimeException(s"Cannot parse null to Boolean")
      case _                                           => throw new RuntimeException(s"Cannot parse '$x' to Boolean")
    }
  }

  implicit val decodeToString: Decoder[Any, String] = Decoder { x: Any =>
    x.toString
  }

  implicit val decodeToAny: Decoder[Any, Any] = Decoder { x: Any =>
    x
  }
}

object DoubleDecoderInstances extends BasicDecoders[Double] {
  implicit override def decodeToDouble = Decoder { d: Double =>
    d
  }
  implicit override def decodeToFloat = Decoder { d: Double =>
    d.toFloat
  }
  implicit override def decodeToInt = Decoder { d: Double =>
    d.toInt
  }
  implicit override def decodeToString = Decoder { d: Double =>
    d.toString
  }
  implicit override def decodeToAny = Decoder { d: Double =>
    d
  }
}

// Hack for String.toInt implicit method
object Helper {
  def strToInt(s: String) = s.toInt
  def strToDouble(s: String) = s.toDouble
  def strToFloat(s: String) = s.toFloat
}
