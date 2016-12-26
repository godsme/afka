package io.darwin.afka.decoder

import java.nio.ByteBuffer

import io.darwin.afka.SchemaException

/**
  * Created by darwin on 24/12/2016.
  */
abstract class WithSizeObjectDecoder[A] extends WithSizeDecoder[A] {

  override def decode(chan: SourceChannel, size: Int): A = {
    if(size < 0) throw SchemaException("size < 0")
    doDecode(chan, size)
  }

  protected def doDecode(chan: SourceChannel, size: Int): A
}
