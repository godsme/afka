package io.darwin.afka.encoder

import io.darwin.afka.SinkChannel

/**
  * Created by darwin on 24/12/2016.
  */
trait KafkaEncoder[A] {
  def encode(ch: SinkChannel, o: A)
}
