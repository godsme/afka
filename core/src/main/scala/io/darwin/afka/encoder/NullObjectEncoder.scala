package io.darwin.afka.encoder

import io.darwin.afka.SinkChannel

/**
  * Created by darwin on 24/12/2016.
  */
trait NullObjectEncoder[A] {
  def encodeNull(ch: SinkChannel)
}
