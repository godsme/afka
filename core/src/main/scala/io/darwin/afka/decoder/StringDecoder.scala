package io.darwin.afka.decoder

import io.darwin.afka.SchemaException

/**
  * Created by darwin on 24/12/2016.
  */
object StringDecoder {

  implicit object WithSizeStringDecoder extends WithSizeObjectDecoder[String] {

    override def doDecode(chan: SourceChannel, size: Int): String = {
      val bytes = new Array[Byte](size)
      chan.getBytes(bytes)
      new String(bytes, "UTF8")
    }

  }

  implicit object WithSizeNullableStringDecoder extends NullableDecoder[String](WithSizeStringDecoder)

  class KafkaStringDecoder[A](implicit decoder: WithSizeDecoder[A])
    extends VarSizeDecoder[A](_.getShort.toInt)
}
