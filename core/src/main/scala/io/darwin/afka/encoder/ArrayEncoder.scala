package io.darwin.afka.encoder

import io.darwin.afka.SinkChannel

/**
  * Created by darwin on 24/12/2016.
  */
class ArrayEncoder[A]( implicit en: KafkaEncoder[A])
  extends KafkaEncoder[Array[A]]{

  override def encode(ch: SinkChannel, o: Array[A]) {
    ch.putInt(o.length)
    o.foreach(en.encode(ch, _))
  }
}
