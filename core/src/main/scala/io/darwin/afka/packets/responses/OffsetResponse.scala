package io.darwin.afka.packets.responses

import io.darwin.kafka.macros.{KafkaResponseElement, KafkaResponsePacket}

/**
  * Created by darwin on 29/12/2016.
  */
@KafkaResponseElement
case class TopicOffsetResponse
  ( partition : Int,
    error     : Short,
    offsets   : Array[Long])

@KafkaResponsePacket
case class OffsetResponse
  ( topics: Array[TopicOffsetResponse] )
