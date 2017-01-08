package io.darwin.afka.decoder

import akka.util.ByteString

/**
  * Created by darwin on 27/12/2016.
  */
object ByteBufferDecoder {
  implicit object WithSizeBytesDecoder extends WithSizeObjectDecoder[ByteString] {
    override def doDecode(chan: SourceChannel, size: Int): ByteString = {
      chan.getByteString(size)
    }
  }

  implicit object WithSizeNullableBytesDecoder extends NullableDecoder[ByteString](WithSizeBytesDecoder)

  private def makeDecoder[A](implicit decoder: WithSizeDecoder[A]) = new VarSizeDecoder[A](_.getInt)(decoder)

  implicit val makeBytesDecoder         = makeDecoder[ByteString]
  implicit val makeNullableBytesDecoder = makeDecoder[Option[ByteString]]
}
