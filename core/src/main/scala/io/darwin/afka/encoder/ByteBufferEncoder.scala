package io.darwin.afka.encoder

import akka.util.ByteString

/**
  * Created by darwin on 27/12/2016.
  */
object ByteBufferEncoder {

  implicit object BYTES extends KafkaEncoder[ByteString] {
    override def encode(ch: SinkChannel, o: ByteString) = {
      ch.putInt(o.length)
      ch.putByteBuffer(o)
    }
  }

  implicit object NULL_BYTES extends NullableEncoder[ByteString]((ch, v) => ch.putInt(v))
}
