package io.darwin.afka.services.domain

import java.net.InetSocketAddress

import akka.actor.{ActorLogging, Props}
import io.darwin.afka.NodeId
import io.darwin.afka.domain.FetchedMessages
import io.darwin.afka.domain.FetchedMessages.{PartitionMessages, TopicMessages}
import io.darwin.afka.domain.GroupOffsets.{NodeOffsets, PartitionOffsetInfo}
import io.darwin.afka.packets.responses._
import io.darwin.afka.services.common.{ChannelAddress, ChannelConnected, KafkaService, KafkaServiceSinkChannel}
import io.darwin.afka.services.pool.{PoolDirectSinkChannel, PoolDynamicSinkChannel}


/**
  * Created by darwin on 30/12/2016.
  */
object FetchService {

  def props( channel    : ChannelAddress,
             clientId   : String,
             offsets    : NodeOffsets ) = {
    Props(classOf[FetchService], channel, clientId, offsets)
  }

  trait Actor extends akka.actor.Actor with ActorLogging {
    this: KafkaServiceSinkChannel {
      val offsets: NodeOffsets
    } ⇒

    log.info("Fetcher restarted")

    override def receive: Receive = {
      case ChannelConnected(_)    ⇒ onConnected
      case msg: FetchResponse     ⇒ onMessageFetched(msg)
    }

    private def onConnected = {
      sending(offsets.toRequest)
    }

    var total = 0
    private def onMessageFetched(msg: FetchResponse) = {
      if(msg.topics.length == 0) {
      }
      else {
        //log.info(s"fetch response received: topics=${msg.topics.length}")
        val msgs = FetchedMessages.decode(1, msg)
        // processingMsgs

        val count = msgs.msgs.foldLeft(0){ case (s, m) ⇒
          s + m.msgs.foldLeft(0) { case (ss, ms) ⇒
            ss + ms.msgs.fold(0)(l ⇒ l.length)
          }
        }

        if(count != 0) {
          total += count
          log.info(s"total = ${total}, count=${count}")
        }

        msgs.msgs.foreach { case TopicMessages(topic, m) ⇒
          m.foreach { case PartitionMessages(partition, error, ms) ⇒
            if (ms.isDefined)
              offsets.updatePartition(topic, partition, PartitionOffsetInfo(ms.get.last.offset, error))
            else
              offsets.updatePartitionError(topic, partition, error)
          }
        }

        sending(offsets.toRequest)
      }
    }
  }
}

class FetchService
  ( val channel    : ChannelAddress,
    val clientId   : String,
    val offsets    : NodeOffsets)
  extends FetchService.Actor with PoolDirectSinkChannel {

  def path: String = "/user/push-service/cluster/broker-service/" + channel.nodeId
}

//class FetchService
//( val channel    : ChannelAddress,
//  val clientId   : String,
//  val offsets    : NodeOffsets)
//  extends FetchService.Actor with KafkaService {
//
//  val remote = new InetSocketAddress(channel.host, channel.port)
//}

