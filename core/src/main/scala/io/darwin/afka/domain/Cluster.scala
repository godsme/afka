package io.darwin.afka.domain

import java.net.InetSocketAddress

import io.darwin.afka.PartitionId
import io.darwin.afka.packets.responses.{BrokerResponse, MetaDataResponse, PartitionMetaData, TopicMetaData}
import io.darwin.afka.services.common.ChannelAddress

/**
  * Created by darwin on 27/12/2016.
  */

////////////////////////////////////////////////////////////////////
class KafkaBroker( val id   : Int,
                   val addr : InetSocketAddress) {
  override val toString = {
    s"broker[${id}] = ${addr.getHostString} : ${addr.getPort}"
  }
}

object KafkaBroker {
  def apply(broker: BrokerResponse) = new KafkaBroker(broker.nodeId, new InetSocketAddress(broker.host, broker.port))
}

////////////////////////////////////////////////////////////////////
class KafkaPartition(val id      : Int,
                     val leader  : Int,
                     val replica : Array[Int],
                     val isr     : Array[Int]) {
  override val toString = {
    s"""partition[${id}] = { leader=${leader}, replicas=${replica.mkString(",")} }"""
  }
}

object KafkaPartition {
  def apply(p: PartitionMetaData) = new KafkaPartition(p.id, p.leader, p.replicas, p.isr)
}

////////////////////////////////////////////////////////////////////
object KafkaTopic {
  def apply(v: TopicMetaData) = new KafkaTopic(
      id         = v.topic,
      partitions = v.partitions.filter(_.leader >= 0).map(KafkaPartition(_)))

  type PartitionMap = Map[PartitionId, KafkaPartition]
}

class KafkaTopic( val id         : String,
                  val partitions : Array[KafkaPartition]) {
  override val toString =
    s"topic[ ${"%-8s".format(id)} ] = ${partitions.mkString(", ")}"

  private var partitionMap: KafkaTopic.PartitionMap = Map.empty

  def toPartitionMap = partitionMap

  partitions
    .filter (_.leader >= 0)
    .foreach(p ⇒ partitionMap += p.id → p)
}

////////////////////////////////////////////////////////////////////
class KafkaCluster( val brokers : Map[Int, KafkaBroker],
                    val topics  : Map[String, KafkaTopic] ) {

  def getMetaByTopic(_topics: Array[String]): KafkaCluster = {
    var newTopics: Map[String, KafkaTopic] = Map.empty

    _topics.foreach { topic ⇒
      topics.get(topic).foreach { content ⇒
        newTopics += topic → content
      }
    }

    new KafkaCluster(brokers, newTopics)
  }

  def getPartitionsByTopic(topic: String): Option[Array[KafkaPartition]] = {
    topics.get(topic).map(_.partitions)
  }

  def getPartitionMapByTopic(topic: String): Option[KafkaTopic.PartitionMap] = {
    topics.get(topic).map(_.toPartitionMap)
  }

  def getBroker(id: Int): Option[ChannelAddress] = {
    brokers.get(id).map(p ⇒ ChannelAddress(id, p.addr.getHostName, p.addr.getPort))
  }

  override val toString = {
    s"${brokers.map(_._2.toString).mkString("\n")}\n\n" +
    s"${topics.map (_._2.toString).mkString("\n")}"
  }
}

object KafkaCluster {
  def apply(meta: MetaDataResponse) = new KafkaCluster(getBrokers(meta), getTopics(meta))

  private def getBrokers(meta: MetaDataResponse) = {
    var nodes: Map[Int, KafkaBroker] = Map.empty

    meta.brokers.foreach { broker ⇒
      nodes += broker.nodeId → KafkaBroker(broker)
    }

    nodes
  }

  private def getTopics(meta: MetaDataResponse) = {
    var topics: Map[String, KafkaTopic] = Map.empty

    meta.topics
      .filter  { t ⇒ !t.isInternal && t.errorCode == 0 }
      .foreach { t ⇒ topics += t.topic → KafkaTopic(t) }

    topics
  }

}

