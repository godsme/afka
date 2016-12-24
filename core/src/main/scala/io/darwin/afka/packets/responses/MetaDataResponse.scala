package io.darwin.afka.packets.responses

import io.darwin.kafka.macros.{KafkaResponse, KafkaResponseElement}

/**
  * Created by darwin on 24/12/2016.
  */
@KafkaResponseElement
case class Broker
  ( nodeId : Int,
    host   : String,
    port   : Int,
    rack   : Option[String])


@KafkaResponseElement
case class PartitionMetaData
  ( errorCode : Short,id        : Int,
    leader    : Int,
    replicas  : Array[Int],
    isr       : Array[Int])

@KafkaResponseElement
case class TopicMetaData
  ( errorCode : Short,topic:      String,
    isInternal: Boolean,
    partitions: Array[PartitionMetaData])

@KafkaResponse
case class MetaDataResponse
  ( brokers      : Array[Broker],
    controllerId : Int,
    topics       : Array[TopicMetaData])

