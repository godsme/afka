package io.darwin.afka.encoder

import io.darwin.afka.SinkChannel

/**
  * Created by darwin on 24/12/2016.
  */
class NullableEncoder[A, B]( implicit en: KafkaEncoder[A],
                             ne: NullObjectEncoder[B])
  extends KafkaEncoder[Option[A]] {

  override def encode(ch: SinkChannel, o: Option[A]) = {
    if(o.isEmpty) ne.encodeNull(ch)
    else en.encode(ch, o.get)
  }

}