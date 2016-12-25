package io.darwin.afka.decoder

import java.nio.ByteBuffer

/**
  * Created by darwin on 24/12/2016.
  */
trait KafkaDecoder[A] {
  def decode(chan: ByteBuffer): A
}
