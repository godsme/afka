package io.darwin.afka.packets.responses

import akka.util.ByteString
import io.darwin.kafka.macros.KafkaResponseElement

/**
  * Created by darwin on 29/12/2016.
  */

@KafkaResponseElement
case class FetchPartitionResponse
  ( partition           : Int,
    error               : Short,
    highWaterMarkOffset : Long,
    messages            : ByteString)

@KafkaResponseElement
case class FetchTopicResponse
  ( topic      : String,
    partitions : Array[FetchPartitionResponse])

@KafkaResponseElement
case class FetchResponse
  ( throttleTime : Int = 0,
    topics       : Array[FetchTopicResponse])
