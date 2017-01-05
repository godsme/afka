package io.darwin.afka.services.domain

import java.net.InetSocketAddress

import akka.actor.{ActorLogging, Props}
import io.darwin.afka.NodeId
import io.darwin.afka.domain.FetchedMessages
import io.darwin.afka.domain.FetchedMessages.{PartitionMessages, TopicMessages}
import io.darwin.afka.domain.GroupOffsets.{NodeOffsets, PartitionOffsetInfo}
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses._
import io.darwin.afka.services.common.{ChannelConnected, KafkaClientConnected, KafkaService}
import io.darwin.afka.services.pool.PoolSinkChannel


/**
  * Created by darwin on 30/12/2016.
  */
object FetchService {

  def props( nodeId     : NodeId,
             clientId   : String,
             offsets    : NodeOffsets ) = {
    Props(classOf[FetchService], nodeId, clientId, offsets)
  }


  trait Actor extends akka.actor.Actor with ActorLogging {
    this: PoolSinkChannel {
      val offsets: NodeOffsets
    } ⇒

    override def receive: Receive = {
      case ChannelConnected       ⇒ onConnected
      case msg: FetchResponse     ⇒ onMessageFetched(msg)
    }

    private def onConnected = {
      sending(offsets.toRequest)
    }

    private def onMessageFetched(msg: FetchResponse) = {
      if(msg.topics.length == 0) {

      }
      else {
        log.info(s"fetch response received: topics=${msg.topics.length}")

        val msgs = FetchedMessages.decode(1, msg)
        // processingMsgs

        msgs.msgs.foreach { case TopicMessages(topic, m) ⇒
          m.foreach { case PartitionMessages(partition, error, ms) ⇒
            if (ms.isDefined)
              offsets.updatePartition(topic, partition, PartitionOffsetInfo(ms.get.last.offset, error))
            else
              offsets.updatePartitionError(topic, partition, error)
          }
        }

        log.info("send fetch request")
        sending(offsets.toRequest)
      }
    }
  }
}

class FetchService
  ( val nodeId     : NodeId,
    val clientId   : String,
    val offsets    : NodeOffsets)
  extends FetchService.Actor with PoolSinkChannel {

  def path: String = "/user/push-service/cluster/broker-service/" + nodeId
}

