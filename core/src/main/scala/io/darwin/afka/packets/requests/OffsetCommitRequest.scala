package io.darwin.afka.packets.requests

import io.darwin.kafka.macros.{KafkaRequestElement, KafkaRequestPacket}

/**
  * Created by darwin on 29/12/2016.
  */
@KafkaRequestElement
case class PartitionOffsetCommitRequest
  ( partition     : Int,
    offset        : Long,
    meta          : Option[String] = None)


@KafkaRequestElement
case class TopicOffsetCommitRequest
  ( topic         : String,
    partitions    : Array[PartitionOffsetCommitRequest])


@KafkaRequestPacket( apiKey = 8, version = 2)
case class OffsetCommitRequest
  ( groupId       : String,
    generation    : Int,
    consumerId    : String,
    retentionTime : Long = -1,
    topics        : Array[TopicOffsetCommitRequest])
