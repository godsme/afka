package io.darwin.afka.domain

import java.net.InetSocketAddress

import io.darwin.afka.packets.responses.{Broker, MetaDataResponse, PartitionMetaData, TopicMetaData}

/**
  * Created by darwin on 27/12/2016.
  */
class KafkaBroker(id: Int, addr: InetSocketAddress) {

  override val toString = {
    s"broker = { id=${id}, addr=${addr.getHostName}:${addr.getPort}}"
  }
}

object KafkaBroker {
  def apply(broker: Broker) = new KafkaBroker(broker.nodeId, new InetSocketAddress(broker.host, broker.port))
}

class KafkaPatition(id: Int, leader: Int, replica: Array[Int]) {

  override val toString = {
    s"""partition = { id=${id}, leader=${leader}, ${replica.foldLeft("replicas="){(s, v) => s + v + ","}} }"""
  }
}

object KafkaPatition {
  def apply(p: PartitionMetaData) = new KafkaPatition(p.id, p.leader, p.replicas)
}

class KafkaTopic(id: String, partions: Array[KafkaPatition]) {

  override val toString = {
    s"topic = { id=${id}, ${partions.foldLeft("paritions = ")((s, p) => s + p.toString + "\n")}}"
  }
}

object KafkaTopic {
  def apply(v: TopicMetaData) = new KafkaTopic(v.topic, v.partitions.map(KafkaPatition(_)))
}

class KafkaCluster(bootstrap: Array[InetSocketAddress]) {

  var nodes: Map[Int, KafkaBroker] = Map.empty
  var topics: Map[String, KafkaTopic] = Map.empty

  def update(meta: MetaDataResponse) = {
    nodes = Map.empty

    meta.brokers.foreach { broker =>
      nodes += broker.nodeId -> KafkaBroker(broker)
    }

    topics = Map.empty
    meta.topics
      .filter { t => !t.isInternal && t.errorCode == 0 }
      .foreach{ t => topics += t.topic -> KafkaTopic(t) }
  }

  override val toString = {
    s"""${nodes.foldLeft("nodes = "){ (s, v) => s + v._2.toString + "\n" }}
        ${topics.foldLeft("topics = ") {(s, v) => s + v._2.toString + "\n"}}
     """
  }

}

