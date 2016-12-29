package io.darwin.afka.packets.requests

import io.darwin.kafka.macros.{KafkaRequestElement, KafkaRequestPacket}

/**
  * Created by darwin on 29/12/2016.
  */
@KafkaRequestElement
case class PartitionOffsetCommitRequest
  ( partition     : Int,
    offset        : Long,
    meta          : String )


@KafkaRequestElement
case class TopicOffsetCommitRequest
  ( topic         : String,
    partitions    : Array[PartitionOffsetCommitRequest])


@KafkaRequestPacket( apiKey = 8, version = 2)
case class OffsetCommitRequest
  ( consumerGroup : String,
    generation    : Int,
    consumerId    : String,
    retentionTime : Long,
    topics        : Array[TopicOffsetCommitRequest])
