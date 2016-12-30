package io.darwin.afka.packets.responses

import io.darwin.kafka.macros.{KafkaResponseElement, KafkaResponsePacket}

/**
  * Created by darwin on 30/12/2016.
  */
@KafkaResponseElement
case class OffsetFetchPartitionResponse
  ( partition : Int,
    offset    : Long,
    meta      : Option[String],
    error     : Short)


@KafkaResponseElement
case class OffsetFetchTopicResponse
  ( topic      : String,
    partitions : Array[OffsetFetchPartitionResponse])


@KafkaResponsePacket
case class OffsetFetchResponse
  ( topics: Array[OffsetFetchTopicResponse] )
