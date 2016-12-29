package io.darwin.afka.packets.responses

import io.darwin.afka.packets.common.ProtoMessageSet
import io.darwin.kafka.macros.KafkaResponseElement

/**
  * Created by darwin on 29/12/2016.
  */

@KafkaResponseElement
case class FetchParitionResponse
  ( partition           : Int,
    error               : Short,
    highWaterMarkOffset : Long,
    messageSize         : Int,
    messages            : ProtoMessageSet)

@KafkaResponseElement
case class FetchTopicResponse
  ( topic:  String,
    partitions: Array[FetchParitionResponse])

@KafkaResponseElement
case class FetchResponse
  ( throttleTime : Int,
    topics       : Array[FetchTopicResponse])
