package io.darwin.afka.domain

import java.net.InetSocketAddress

import io.darwin.afka.packets.responses.{Broker, MetaDataResponse, PartitionMetaData, TopicMetaData}

/**
  * Created by darwin on 27/12/2016.
  */

////////////////////////////////////////////////////////////////////
class KafkaBroker( val id   : Int,
                   val addr : InetSocketAddress) {
  override val toString = {
    s"broker[${id}] = { addr=${addr.getHostString}:${addr.getPort}}"
  }
}

object KafkaBroker {
  def apply(broker: Broker) = new KafkaBroker(broker.nodeId, new InetSocketAddress(broker.host, broker.port))
}

////////////////////////////////////////////////////////////////////
class KafkaPatition( val id      : Int,
                     val leader  : Int,
                     val replica : Array[Int]) {
  override val toString = {
    s"""partition[${id}] = { leader=${leader}, replicas=${replica.mkString(",")} }"""
  }
}

object KafkaPatition {
  def apply(p: PartitionMetaData) = new KafkaPatition(p.id, p.leader, p.replicas)
}

////////////////////////////////////////////////////////////////////
class KafkaTopic( val id       : String,
                  val partions : Array[KafkaPatition]) {
  override val toString = {
    s"topic[${id}] = { ${partions.mkString(", ")}"
  }
}

object KafkaTopic {
  def apply(v: TopicMetaData) = new KafkaTopic(v.topic, v.partitions.map(KafkaPatition(_)))
}

////////////////////////////////////////////////////////////////////
class KafkaCluster( val brokers : Map[Int, KafkaBroker],
                    val topics  : Map[String, KafkaTopic] ) {
  override val toString = {
    s"${brokers.map(_._2.toString).mkString("\n")}\n\n" +
    s"${topics.map(_._2.toString).mkString("\n")}"
  }
}

object KafkaCluster {
  def apply(meta: MetaDataResponse) = new KafkaCluster(getBrokers(meta), getTopics(meta))

  private def getBrokers(meta: MetaDataResponse) = {
    var nodes: Map[Int, KafkaBroker] = Map.empty

    meta.brokers.foreach { broker =>
      nodes += broker.nodeId -> KafkaBroker(broker)
    }

    nodes
  }

  private def getTopics(meta: MetaDataResponse) = {
    var topics: Map[String, KafkaTopic] = Map.empty

    meta.topics
      .filter { t => !t.isInternal && t.errorCode == 0 }
      .foreach{ t => topics += t.topic -> KafkaTopic(t) }

    topics
  }
}
