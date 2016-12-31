package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{ActorLogging, Props}
import akka.util.ByteString
import io.darwin.afka.PartitionId
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses._
import io.darwin.afka.decoder.decoding
import io.darwin.afka.packets.common.ProtoMessageInfo

import scala.collection.mutable.MutableList


/**
  * Created by darwin on 30/12/2016.
  */
object FetchService {

  def props( remote     : InetSocketAddress,
             clientId   : String,
             groupId    : String,
             memberId   : String,
             generation : Int,
             topics     : Array[TopicAssignment]) = {
    Props(classOf[FetchService], remote, clientId, groupId, memberId, generation, topics)
  }

  case class TopicAssignment(topic: String, partitions: Array[PartitionId])

  case class PartitionMessages(parition: Int, msgs: MutableList[ProtoMessageInfo])
  case class TopicMessages(topic: String, msgs: MutableList[PartitionMessages])
  case class NodeMessages(node: Int, msgs: MutableList[TopicMessages])

  trait Actor extends KafkaActor with ActorLogging {
    this: Actor with KafkaService {
      val memberId   : String
      val generation : Int
      val groupId    : String
      val topics     : Array[TopicAssignment]
    } ⇒

    override def receive: Receive = {
   //   case KafkaClientConnected(_)       ⇒ onConnected
      case offset : OffsetFetchResponse  ⇒ onOffsetFetched(offset)
      case msg    : FetchResponse        ⇒ onMessageFetched(msg)
      case commit : OffsetCommitResponse ⇒ onCommitted(commit)
    }

//    private def onConnected = {
//      send(OffsetFetchRequest(
//        group  = groupId,
//        topics = topics.map {
//          case TopicAssignment(topic, partitions) ⇒ OffsetFetchTopicRequest(topic, partitions)
//      }))
//    }

    private def onOffsetFetched(offset: OffsetFetchResponse) = {
      offset.topics.foreach {
        case OffsetFetchTopicResponse(topic, partitions) ⇒
          partitions.filter(_.error != KafkaErrorCode.NO_ERROR).foreach {
            case OffsetFetchPartitionResponse(part, offset, _, error) ⇒
              log.info(s"fetch offset error = ${error} on ${part}:${offset}")
          }
      }
      val t: Array[FetchTopicRequest] = offset.topics.map {
        case OffsetFetchTopicResponse(topic, partitions) ⇒
          FetchTopicRequest(
            topic      = topic,
            partitions = partitions.filter(p ⇒ p.error == KafkaErrorCode.NO_ERROR && p.offset >= 0).map {
              case OffsetFetchPartitionResponse(partition, offset, _, _) ⇒
                FetchPartitionRequest(partition, offset)
            })
      }

      val r = t.filter(_.partitions.size > 0)
      if(r.size > 0) {
        val s: Array[String] = r.map(s ⇒ s.topic + " : " + s.partitions.map(p ⇒ p.partition + ":" + p.offset).mkString(","))
        log.info(s"send fetch request: ${s.mkString(",")}")

        send(FetchRequest(topics = r))
      }
    }

    private def onMessageFetched(msg: FetchResponse) = {
      log.info("fetch response received")

      def decodeMsgs(msgs: ByteString) = {
        var partitionMsgs = new MutableList[ProtoMessageInfo]
        log.info(s"total size = ${msgs.size}")
        val chan = ByteStringSourceChannel(msgs)
        while(chan.remainSize > 0) {
          partitionMsgs += decoding[ProtoMessageInfo](chan)
        }
        log.info(s"num of msgs = ${partitionMsgs.size}")
        partitionMsgs
      }

      def decodeTopicMsgs(partitions: Array[FetchPartitionResponse]) = {
        val topicMsgs = new MutableList[PartitionMessages]
        partitions.foreach {
          case FetchPartitionResponse(partition, error, wm, msgs) ⇒
            if(error == KafkaErrorCode.NO_ERROR && msgs.size > 0) {
              topicMsgs += PartitionMessages(partition, decodeMsgs(msgs))
            }

        }
        topicMsgs
      }

      def decodeNodeMsgs = {
        val nodeMsgs = new MutableList[TopicMessages]
        msg.topics.foreach {
          case FetchTopicResponse(topic, partitions) ⇒
            val topicMsgs = decodeTopicMsgs(partitions)
            if (topicMsgs.size > 0) {
              nodeMsgs += TopicMessages(topic, topicMsgs)
            }
        }
        nodeMsgs
      }

      def sendCommit(nodeMsgs: MutableList[TopicMessages]) = {
        send(OffsetCommitRequest(
          groupId = groupId,
          generation = generation,
          consumerId = memberId,
          topics = nodeMsgs.toArray.map {
            case TopicMessages(topic, msgs) ⇒
              TopicOffsetCommitRequest(topic, msgs.toArray.map {
                case PartitionMessages(partition, infos) ⇒
                  log.info(s"commit partition = ${partition}, offset = ${infos.last.offset}")
                  PartitionOffsetCommitRequest(partition, infos.last.offset+1)
              })
          }))
      }

      val nodeMsgs = decodeNodeMsgs
      if(nodeMsgs.size > 0) {
        sendCommit(nodeMsgs)
      }
    }

    def onCommitted(commit: OffsetCommitResponse) = {
      commit.topics.foreach {
        case OffsetCommitTopicResponse(topic, partitions) ⇒
          log.info(s"commit topic: ${topic}")
          partitions.foreach {
            case OffsetCommitPartitionResponse(partition, error) ⇒
              log.info(s"partition = ${partition}, error=${error}")
          }
      }
    }
  }
}

import io.darwin.afka.services.FetchService.TopicAssignment

class FetchService
  ( val remote     : InetSocketAddress,
    val clientId   : String,
    val groupId    : String,
    val memberId   : String,
    val generation : Int,
    val topics     : Array[TopicAssignment])
  extends KafkaActor with FetchService.Actor with KafkaService
