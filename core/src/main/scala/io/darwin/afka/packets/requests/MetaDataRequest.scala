package io.darwin.afka.packets.requests

import io.darwin.afka.encoder.{KafkaEncoder, SinkChannel, _}
import io.darwin.kafka.macros.KafkaRequest

/**
  * Created by darwin on 24/12/2016.
  */
@KafkaRequest(apiKey = 1, version = 1)
case class MetaDataRequest
  ( val topics: Option[Array[String]],val name: Byte )


object MetaDataRequestEncoder extends KafkaEncoder[MetaDataRequest] {
  override def encode(ch: SinkChannel, o: MetaDataRequest) = {
    encoding(ch, o.topics)
    encoding(ch, o.name)
  }
}