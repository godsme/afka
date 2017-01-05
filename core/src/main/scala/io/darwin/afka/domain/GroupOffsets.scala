package io.darwin.afka.domain

import java.util.NoSuchElementException

import io.darwin.afka.{NodeId, PartitionId, TopicId}
import io.darwin.afka.packets.common.ProtoPartitionAssignment
import io.darwin.afka.packets.requests.{FetchPartitionRequest, FetchRequest, FetchTopicRequest, OffsetFetchRequest}
import io.darwin.afka.packets.responses.{OffsetFetchPartitionResponse, OffsetFetchResponse, OffsetFetchTopicResponse}

import scala.collection.mutable.Map

/**
  * Created by darwin on 31/12/2016.
  */

object GroupOffsets {

  def apply(cluster: KafkaCluster, group: Array[ProtoPartitionAssignment]): GroupOffsets =
    new GroupOffsets(cluster, group)

  case class PartitionOffsetInfo(offset: Long, error: Short)
  case class PartitionOffset(parition: Int, info: Option[PartitionOffsetInfo] = None) {
    override def toString = {
      s"info = ${info.fold("None")(p ⇒ s"offset=${p.offset}, error=${p.error}")}"
    }
  }

  case class TopicOffsets private[GroupOffsets](topic: TopicId) {
    type PartitionMap = Map[PartitionId, PartitionOffset]

    private val offsets: PartitionMap = Map.empty

    private[GroupOffsets] def addPartition(partition: Int) = {
      offsets.getOrElseUpdate(partition, PartitionOffset(partition))
    }

    def updatePartition(partition: PartitionId, offset: PartitionOffsetInfo): Boolean = {
      offsets.get(partition) match {
        case None ⇒ false
        case Some(_) ⇒
          offsets(partition) = PartitionOffset(partition, Some(offset))
          true
      }
    }

    def updatePartitionError(partition: PartitionId, error: Short): Boolean = {
      offsets.get(partition) match {
        case None ⇒ false
        case Some(p) ⇒
          offsets(partition) = PartitionOffset(partition, Some(PartitionOffsetInfo(p.info.get.offset, error)))
          true
      }
    }

    def toRequest = {
      offsets.toArray.filter{case (_, p) ⇒ p.info.isDefined && p.info.get.error == 0}.map {
        case (id, p) ⇒ FetchPartitionRequest(id, p.info.get.offset + 1)
      }
    }

    override def toString = {
      offsets.map {
        case (p, o) ⇒ s"[partition=${p}, ${o.toString}]"
      }.mkString(",")
    }

  }

  case class NodeOffsets(nodeId: Int) {
    type TopicMap = Map[TopicId, TopicOffsets]

    private val offsets: TopicMap = Map.empty

    private[GroupOffsets] def addTopic(topic: TopicId): TopicOffsets = {
      offsets.getOrElseUpdate(topic, TopicOffsets(topic))
    }

    def updatePartition(topic: TopicId, partition: PartitionId, offset: PartitionOffsetInfo): Boolean = {
      offsets.get(topic) match {
        case None ⇒ false
        case Some(p) ⇒ p.updatePartition(partition, offset)
      }
    }

    def updatePartitionError(topic: TopicId, partition: PartitionId, error: Short): Boolean = {
      offsets.get(topic) match {
        case None ⇒ false
        case Some(p) ⇒ p.updatePartitionError(partition, error)
      }
    }

    def updateOffset(topic: TopicId, partition: PartitionId, offset: PartitionOffsetInfo): Boolean = {
      offsets.get(topic) match {
        case None ⇒ false
        case Some(p) ⇒ p.updatePartition(partition, offset)
      }
    }

    def toRequest = {
      FetchRequest(
        topics = offsets.toArray.map { case (topic, part) ⇒
          FetchTopicRequest(topic, part.toRequest)
        })
    }

    override def toString: String = {
      offsets.map {
        case (topic, off) ⇒ s"topic = ${topic}, ${off.toString}"
      }.mkString(",")
    }
  }

  class GroupOffsets(cluster: KafkaCluster, group: Array[ProtoPartitionAssignment]) {
    type NodeMap = Map[NodeId, NodeOffsets]

    val offsets: NodeMap = Map.empty

    group.foreach {
      case ProtoPartitionAssignment(topic, partitions) ⇒
        cluster
          .getPartitionMapByTopic(topic)
          .foreach { map ⇒
            partitions.foreach { pid ⇒
              map.get(pid).foreach { p ⇒
                addNode(p.leader).addTopic(topic).addPartition(pid)
              }
            }
          }

    }

    println(cluster.toString)

    private def addNode(node: NodeId): NodeOffsets = {
      offsets.getOrElseUpdate(node, NodeOffsets(node))
    }

    def getNodeById(node: NodeId): Option[NodeOffsets] =  offsets.get(node)

    def updatePartition(topic: TopicId, partition: PartitionId, offset: PartitionOffsetInfo): Boolean = {
      object Found extends Exception { }

      try {
        offsets.foreach {
          case (_, node) ⇒ if(node.updatePartition(topic, partition, offset)) throw Found
        }
      }
      catch {
        case Found ⇒ return true
      }

      false
    }

    def update(r: Array[OffsetFetchTopicResponse]) = {
      r.foreach {
        case OffsetFetchTopicResponse(topic, parts) ⇒
          parts.foreach {
            case OffsetFetchPartitionResponse(p, offset, _, error) ⇒
              updatePartition(topic, p, GroupOffsets.PartitionOffsetInfo(offset, error))
          }
      }

      offsets.foreach {
        case (node, n) ⇒ println(s"node = ${node}, ${n.toString}")
      }
    }

    def toRequests = {
      offsets.toArray.map {
        case (node, p) ⇒ (node, p.toRequest)
      }
    }
  }

}