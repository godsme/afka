package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{ActorLogging, Props}
import io.darwin.afka.PartitionId
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses._
import io.darwin.afka.decoder.decoding
import io.darwin.afka.packets.common.{ProtoMessageInfo, ProtoMessageSet}


/**
  * Created by darwin on 30/12/2016.
  */
object FetchService {

  def props( remote   : InetSocketAddress,
             clientId : String,
             groupId  : String,
             topics   : Array[TopicAssignment]) = {
    Props(classOf[FetchService], remote, clientId, groupId, topics)
  }

  case class TopicAssignment(topic: String, partitions: Array[PartitionId])

  trait Actor extends KafkaActor with ActorLogging {
    this: Actor with KafkaService {
      val groupId  : String
      val topics   : Array[TopicAssignment]
    } ⇒

    override def receive: Receive = {
      case KafkaClientConnected(_)      ⇒ onConnected
      case offset : OffsetFetchResponse ⇒ onOffsetFetched(offset)
      case msg    : FetchResponse       ⇒ onMessageFetched(msg)
    }

    private def onConnected = {
      send(OffsetFetchRequest(
        group  = groupId,
        topics = topics.map {
          case TopicAssignment(topic, partitions) ⇒ OffsetFetchTopicRequest(topic, partitions)
      }))
    }

    private def onOffsetFetched(offset: OffsetFetchResponse) = {
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
      msg.topics.foreach {
        case FetchTopicResponse(topic, partitions) ⇒
          log.info(s"topic=$topic")
          partitions.foreach {
            case FetchPartitionResponse(partition, error, wm, msgs) ⇒
              log.info(s"partion = ${partition}, error = ${error}, wm=${wm}, msgs=${msgs.size}")
              if(msgs.size > 0) {
                var s = 0
                val chan = ByteStringSourceChannel(msgs)
                while(chan.remainSize > 0) {
                  val mm = decoding[ProtoMessageInfo](chan)
                  log.info(s"offset = ${mm.offset}, msgs = ${mm.msgSize}")
                  s += 1
                }
                log.info(s"total msgs = ${s}")
              }
          }

      }
    }
  }
}

import io.darwin.afka.services.FetchService.TopicAssignment

class FetchService
  ( val remote   : InetSocketAddress,
    val clientId : String,
    val groupId  : String,
    val topics   : Array[TopicAssignment])
  extends KafkaActor with FetchService.Actor with KafkaService
