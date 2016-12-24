package io.darwin.afka

import io.darwin.afka.decoder.StringDecoder._

/**
  * Created by darwin on 24/12/2016.
  */
package object decoder {

  implicit object STRING extends KafkaStringDecoder[String]

  implicit object NULLABLE_STRING extends KafkaStringDecoder[Option[String]]

  implicit object BOOLEAN extends KafkaDecoder[Boolean] {
    override def decode(chan: SourceChannel): Boolean = 0 != chan.getByte
  }

  implicit object INT8 extends KafkaDecoder[Byte] {
    override def decode(chan: SourceChannel): Byte = chan.getByte
  }

  implicit object INT16 extends KafkaDecoder[Short] {
    override def decode(chan: SourceChannel): Short = chan.getShort
  }

  implicit object INT32 extends KafkaDecoder[Int] {
    override def decode(chan: SourceChannel): Int = chan.getInt
  }

  implicit object INT64 extends KafkaDecoder[Long] {
    override def decode(chan: SourceChannel): Long = chan.getLong
  }

  implicit val ARRAY_INT8 = ArrayDecoder.make[Byte]
  implicit val ARRAY_INT32 = ArrayDecoder.make[Int]

  def decoding[A](chan: SourceChannel)(implicit decoder: KafkaDecoder[A]): A = {
    decoder.decode(chan)
  }
}
