package io.darwin.afka.decoder

import java.nio.ByteBuffer

/**
  * Created by darwin on 24/12/2016.
  */
trait WithSizeDecoder[A] {
  def decode(chan: ByteBuffer, size: Int): A
}
