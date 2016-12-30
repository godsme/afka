package io.darwin.afka.packets.requests

import io.darwin.kafka.macros.{KafkaRequestElement, KafkaRequestPacket}

/**
  * Created by darwin on 30/12/2016.
  */
@KafkaRequestElement
case class OffsetFetchTopicRequest
  ( topic      : String,
    partitions : Array[Int])

@KafkaRequestPacket(apiKey = 9, version = 1)
case class OffsetFetchRequest
  ( group  : String,
    topics : Array[OffsetFetchTopicRequest])

