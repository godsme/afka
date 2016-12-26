package io.darwin.afka.decoder

import java.nio.ByteBuffer

/**
  * Created by darwin on 24/12/2016.
  */
class NullableDecoder[A](decoder: WithSizeDecoder[A])
  extends WithSizeDecoder[Option[A]] {

  override def decode(chan: SourceChannel, size: Int): Option[A] = {
    if(size < 0) None
    else Some(decoder.decode(chan, size))
  }

}
