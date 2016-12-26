package io.darwin.afka.packets.responses

import io.darwin.kafka.macros.{KafkaResponsePacket, KafkaResponseElement}

/**
  * Created by darwin on 24/12/2016.
  */
@KafkaResponseElement
case class Broker
  ( nodeId : Int,
    host   : String,
    port   : Int,
    rack   : Option[String]) {

  override def toString: String = {
    "nodeId : " + nodeId + "\n" +
    "host   : " + host + "\n" +
    "port   : " + port + "\n" +
    "rack   : " + rack.toString
  }
}

@KafkaResponseElement
case class PartitionMetaData
  ( errorCode : Short,
    id        : Int, leader    : Int,
    replicas  : Array[Int],
    isr       : Array[Int]) {

  override val toString = {
    s"error  = ${errorCode}, id = ${id}, leader = ${leader}, " +
    replicas.foldLeft("replicas = ")((s, r) => s + r + ", ") +
    isr.foldLeft("isr = ")((s, i) => s + i + ", ")
  }
}


@KafkaResponseElement
case class TopicMetaData
  ( errorCode  : Short,
    topic      : String,
    isInternal : Boolean,
    partitions : Array[PartitionMetaData]) {

  override val toString = {
    s"error      = ${errorCode}\n" +
    s"topic      = ${topic}\n" +
    s"internal   = ${isInternal}\n" +
    partitions.foldLeft("partitions = ")((s, p) => s + s"${p.toString}\n" )
  }
}


@KafkaResponsePacket
case class MetaDataResponse
  ( brokers      : Array[Broker],
    controllerId : Int,
    topics       : Array[TopicMetaData]) {

  override val toString = {
    brokers.foldLeft("brokers : \n") { (s, broker) => s + broker.toString + "\n" } +
    s"controller-id :  ${controllerId} \n" +
    topics.foldLeft("topics : ") { (s, topic) => s + s"${topic.toString}\n" }
  }

}

