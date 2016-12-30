package io.darwin.afka.packets.requests

import io.darwin.kafka.macros.{KafkaPacketElement, KafkaRequestElement, KafkaRequestPacket, KafkaResponseElement}

/**
  * Created by darwin on 29/12/2016.
  */

@KafkaRequestElement
case class FetchPartitionRequest
  ( partition   : Int,
    offset      : Long,
    maxBytes    : Int  = 64 * 1024)

@KafkaRequestElement
case class FetchTopicRequest
  ( topic       : String,
    partitions  : Array[FetchPartitionRequest])


@KafkaRequestPacket(apiKey = 1, version = 3)
case class FetchRequest
  ( replica     : Int = -1,
    maxWaitTime : Int = 1000,
    minBytes    : Int = 1, //8 * 1024,
    maxBytes    : Int = 64 * 1024,
    topics      : Array[FetchTopicRequest])
