package io.darwin.afka

import java.nio.ByteBuffer

/**
  * Created by darwin on 24/12/2016.
  */
package object encoder {

  implicit object StringEncoder extends KafkaEncoder[String] {

    override def encode(ch: SinkChannel, o: String) = {
      val bytes: Array[Byte] = o.getBytes("UTF8")

      if (bytes.length > Short.MaxValue) {
        throw SchemaException("string is too long")
      }

      ch.putShort(bytes.length.toShort)
      ch.putBytes(bytes)
    }

  }

  implicit object ByteBufferEncoder extends KafkaEncoder[ByteBuffer] {
    override def encode(ch: SinkChannel, o: ByteBuffer) = {
      val pos = o.position()

      ch.putInt(o.remaining)
      ch.putByteBuffer(o)

      o.position(pos)
    }
  }

  implicit object BoolEncoder extends KafkaEncoder[Boolean] {
    override def encode(ch: SinkChannel, o: Boolean) = {
      ch.putByte((if(o) 1 else 0).toByte)
    }
  }

  implicit object ByteEncoder extends KafkaEncoder[Byte] {
    override def encode(ch: SinkChannel, o: Byte) = {
      ch.putByte(o)
    }
  }

  implicit object ShortEncoder extends KafkaEncoder[Short] {
    override def encode(ch: SinkChannel, o: Short) = {
      ch.putShort(o)
    }
  }

  implicit object IntEncoder extends KafkaEncoder[Int] {
    override def encode(ch: SinkChannel, o: Int) = {
      ch.putInt(o)
    }
  }

  implicit object LongEncoder extends KafkaEncoder[Long] {
    override def encode(ch: SinkChannel, o: Long) = {
      ch.putLong(o)
    }
  }

  implicit object ShortNullEncoder extends NullObjectEncoder[Short] {
    override def encodeNull(ch: SinkChannel) = ch.putShort(-1)
  }

  implicit object IntNullEncoder extends NullObjectEncoder[Int] {
    override def encodeNull(ch: SinkChannel) = ch.putInt(-1)
  }

  implicit object StringArrayEncoder extends ArrayEncoder[String]

  implicit object NullableArrayOfStringEncoder extends NullableEncoder[Array[String], Short]

  implicit object NullableByteBufferEncoder extends NullableEncoder[ByteBuffer, Int]

  def encoding[A](ch: SinkChannel, o: A)(implicit encoder: KafkaEncoder[A]) = {
    encoder.encode(ch, o)
  }

}
