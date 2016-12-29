package io.darwin.afka

import akka.util.ByteString
import io.darwin.afka.services.ByteStringSinkChannel


/**
  * Created by darwin on 24/12/2016.
  */
package object encoder {

  implicit object STRING extends KafkaEncoder[String] {

    override def encode(ch: SinkChannel, o: String) = {
      val bytes: Array[Byte] = o.getBytes("UTF8")

      if (bytes.length > Short.MaxValue) {
        throw SchemaException("string is too long")
      }

      ch.putShort(bytes.length.toShort)
      ch.putBytes(bytes)
    }

  }

  implicit object BOOLEAN extends KafkaEncoder[Boolean] {
    override def encode(ch: SinkChannel, o: Boolean) = {
      ch.putByte((if(o) 1 else 0).toByte)
    }
  }

  implicit object INT8 extends KafkaEncoder[Byte] {
    override def encode(ch: SinkChannel, o: Byte) = ch.putByte(o)
  }

  implicit object INT16 extends KafkaEncoder[Short] {
    override def encode(ch: SinkChannel, o: Short) = ch.putShort(o)
  }

  implicit object INT32 extends KafkaEncoder[Int] {
    override def encode(ch: SinkChannel, o: Int) = ch.putInt(o)
  }

  implicit object INT64 extends KafkaEncoder[Long] {
    override def encode(ch: SinkChannel, o: Long) = ch.putLong(o)
  }

  implicit object INT8_ARRAY extends ArrayEncoder[Byte]
  implicit object INT16_ARRAY extends ArrayEncoder[Short]
  implicit object INT32_ARRAY extends ArrayEncoder[Int]
  implicit object INT64_ARRAY extends ArrayEncoder[Long]

  implicit object NULL_INT8_ARRAY extends NullableArrayEncoder[Byte]
  implicit object NULL_INT16_ARRAY extends NullableArrayEncoder[Short]
  implicit object NULL_INT32_ARRAY extends NullableArrayEncoder[Int]
  implicit object NULL_INT64_ARRAY extends NullableArrayEncoder[Long]

  implicit object NULL_STRING extends NullableEncoder[String]((chan, v) => chan.putShort(v.toShort))
  implicit object STRING_ARRAY extends ArrayEncoder[String]
  implicit object NULL_STRING_ARRAY extends NullableArrayEncoder[String]

  implicit val BYTES = ByteBufferEncoder.BYTES
  implicit val NULL_BYTES = ByteBufferEncoder.NULL_BYTES

  def encoding[A](ch: SinkChannel, o: A)(implicit encoder: KafkaEncoder[A]) = encoder.encode(ch, o)

  def encode[A](o: A)(implicit encoder: KafkaEncoder[A]): ByteString = {
    ByteStringSinkChannel().encodeWithoutSize(o)
  }
}
