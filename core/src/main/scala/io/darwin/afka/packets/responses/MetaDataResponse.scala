package io.darwin.afka.packets.responses

import io.darwin.kafka.macros.{KafkaResponsePacket, KafkaResponseElement}

/**
  * Created by darwin on 24/12/2016.
  */
@KafkaResponseElement
case class BrokerResponse
  ( nodeId : Int,
    host   : String,
    port   : Int,
    rack   : Option[String])

@KafkaResponseElement
case class PartitionMetaData
  ( errorCode : Short,
    id        : Int,
    leader    : Int,
    replicas  : Array[Int],
    isr       : Array[Int])


@KafkaResponseElement
case class TopicMetaData
  ( errorCode  : Short,
    topic      : String,
    isInternal : Boolean,
    partitions : Array[PartitionMetaData])


@KafkaResponsePacket
case class MetaDataResponse
  (brokers      : Array[BrokerResponse],
   clusterId    : Option[String],
   controllerId : Int,
   topics       : Array[TopicMetaData])

