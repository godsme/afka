package io.darwin.afka.packets.requests

import io.darwin.kafka.macros.{KafkaPacketElement, KafkaRequestPacket}

/**
  * Created by darwin on 29/12/2016.
  */

@KafkaPacketElement
case class FetchPartionRequest
  ( partition   : Int,
    offset      : Long,
    maxBytes    : Int )

@KafkaPacketElement
case class FetchTopicRequest
  ( topic       : String,
    partitions  : Array[FetchPartionRequest])


@KafkaRequestPacket(apiKey = 1, version = 1)
case class FetchRequest
  ( replica     : Int,
    maxWaitTime : Int,
    minBytes    : Int,
    topics      : Array[FetchTopicRequest])
