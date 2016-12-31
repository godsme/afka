package io.darwin.afka.packets.responses

import io.darwin.kafka.macros.{KafkaResponseElement, KafkaResponsePacket}

/**
  * Created by darwin on 30/12/2016.
  */
@KafkaResponseElement
case class OffsetCommitPartitionResponse
  ( partition : Int,
    error     : Short)

@KafkaResponseElement
case class OffsetCommitTopicResponse
  ( topic: String,
    partitions: Array[OffsetCommitPartitionResponse])

@KafkaResponsePacket
case class OffsetCommitResponse
  ( topics: Array[OffsetCommitTopicResponse])