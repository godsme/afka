package io.darwin.afka.packets.requests

import io.darwin.kafka.macros.{KafkaRequestElement, KafkaRequestPacket}

/**
  * Created by darwin on 29/12/2016.
  */

@KafkaRequestElement
case class PartitionOffsetRequest
  ( partition  : Int,
    time       : Long,
    maxNum     : Int)


@KafkaRequestElement
case class TopicOffsetRequest
  ( topic      : String,
    partitions : Array[PartitionOffsetRequest])


@KafkaRequestPacket(apiKey = 2, version = 0)
case class OffsetRequest
  ( replicaId  : Int = -1,
    topics     : Array[TopicOffsetRequest])
