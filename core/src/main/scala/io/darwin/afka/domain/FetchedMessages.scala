package io.darwin.afka.domain

import akka.util.ByteString
import io.darwin.afka.{PartitionId, SchemaException}
import io.darwin.afka.decoder.decoding
import io.darwin.afka.packets.common.ProtoMessageInfo
import io.darwin.afka.packets.responses.{FetchPartitionResponse, FetchResponse, FetchTopicResponse, KafkaErrorCode}
import io.darwin.afka.services.ByteStringSourceChannel

import scala.collection.mutable.MutableList

/**
  * Created by darwin on 31/12/2016.
  */
object FetchedMessages {

  case class PartitionMessages(parition: Int, error: Short, msgs: Option[MutableList[ProtoMessageInfo]])
  case class TopicMessages(topic: String, msgs: MutableList[PartitionMessages])
  case class NodeMessages(node: Int, msgs: MutableList[TopicMessages])

  def decode(nodeId: Int, msg: FetchResponse): NodeMessages = {
    //println(s"fetch response received: topics=${msg.topics.length}")

    def decodeMsgs(partition: PartitionId, msgs: ByteString) = {
      var partitionMsgs = new MutableList[ProtoMessageInfo]
      //println(s"total size = ${msgs.size}")
      val chan = ByteStringSourceChannel(msgs)
      var before = 0
      while(chan.remainSize > 0) {
        //println(s"${chan.remainSize}")
        try {
          before = chan.remainSize
          partitionMsgs += decoding[ProtoMessageInfo](chan)
        } catch {
          case e: NoSuchElementException ⇒ ()
        }
      }
      println(s"${partition}: # of msgs = ${partitionMsgs.size}")
      partitionMsgs
    }

    def decodeTopicMsgs(partitions: Array[FetchPartitionResponse]) = {
      val topicMsgs = new MutableList[PartitionMessages]
      partitions.foreach {
        case FetchPartitionResponse(partition, error, wm, msgs) ⇒
          if(error == KafkaErrorCode.NO_ERROR) {
            if(msgs.size > 0)
              topicMsgs += PartitionMessages(partition, 0, Some(decodeMsgs(partition, msgs)))
          }
          else
            topicMsgs += PartitionMessages(partition, error, None)
      }
      topicMsgs
    }

    val nodeMsgs = new MutableList[TopicMessages]
    msg.topics.foreach {
      case FetchTopicResponse(topic, partitions) ⇒
        val topicMsgs = decodeTopicMsgs(partitions)
        if (topicMsgs.size > 0) {
          nodeMsgs += TopicMessages(topic, topicMsgs)
        }
    }

    val total = nodeMsgs.foldLeft(0) {
      case (size, TopicMessages(_, msgs)) ⇒ size + msgs.foldLeft(0) {
        case (s, PartitionMessages(_, e, Some(m))) ⇒ s + m.size
        case (s, _) ⇒ s
      }
    }

    //println(s"total num of msgs = ${total}")

    NodeMessages(nodeId, nodeMsgs)
  }
}
