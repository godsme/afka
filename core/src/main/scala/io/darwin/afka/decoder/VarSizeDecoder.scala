package io.darwin.afka.decoder

import java.nio.ByteBuffer

/**
  * Created by darwin on 24/12/2016.
  */
class VarSizeDecoder[A]
    ( read: ByteBuffer => Int )
    ( implicit decoder: WithSizeDecoder[A] )
  extends KafkaDecoder[A] {

  override def decode(ch: ByteBuffer): A = {
    decoder.decode(ch, read(ch))
  }
}

