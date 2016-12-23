package io.darwin.afka.packets

import io.darwin.afka.SinkChannel
import io.darwin.afka.encoder.KafkaEncoder
import io.darwin.macros.KafkaRequest
import io.darwin.afka.encoder._

/**
  * Created by darwin on 24/12/2016.
  */
@KafkaRequest(key = 1)
case class MetaDataRequest
  ( val topics: Option[Array[String]],
    val name: Byte )


object MetaDataRequestEncoder extends KafkaEncoder[MetaDataRequest] {
  override def encode(ch: SinkChannel, o: MetaDataRequest) = {
    encoding(ch, o.topics)
    encoding(ch, o.name)
  }
}