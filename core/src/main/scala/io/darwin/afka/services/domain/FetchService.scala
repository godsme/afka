package io.darwin.afka.services.domain

import java.net.InetSocketAddress

import akka.actor.{ActorLogging, ActorRef, Cancellable, Props, Terminated}
import io.darwin.afka.domain.FetchedMessages
import io.darwin.afka.domain.FetchedMessages.{PartitionMessages, TopicMessages}
import io.darwin.afka.domain.GroupOffsets.{NodeOffsets, PartitionOffsetInfo}
import io.darwin.afka.packets.responses._
import io.darwin.afka.services.common.{ChannelAddress, ChannelConnected, KafkaService, KafkaServiceSinkChannel}
import io.darwin.afka.services.pool.PoolDirectSinkChannel

import scala.concurrent.duration._

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

    var broker: Option[ActorRef] = None

    override def receive: Receive = {
      case ChannelConnected(broker)  ⇒ sendRequest
      case msg: FetchResponse        ⇒ onMessageFetched(msg)
      case Terminated(_)             ⇒
        log.error("CONNECTION LOST")
        context stop self
    }

    var responded = true
    var timer: Option[Cancellable] = None


    private def sendRequest = {
      sending(offsets.toRequest)
    }

    private def startTimer = {
      responded = false
      import scala.concurrent.ExecutionContext.Implicits.global
      timer = Some(context.system.scheduler.scheduleOnce(20 second) {
        if(!responded) {
          log.error("no response from broker")
          context stop self
        }
      })

    }
    private def stopTimer = {
      responded = true
      timer.foreach(_.cancel)
      timer = None
    }

    private var total = 0

    private def onMessageFetched(msg: FetchResponse) = {
      if(msg.topics.length > 0) {
        val msgs = FetchedMessages.decode(1, msg)

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
            ms.fold
            { offsets.updatePartitionError(topic, partition, error) }
            { mss ⇒ offsets.updatePartition(topic, partition, PartitionOffsetInfo(mss.last.offset, error)) }
          }
        }

        sendRequest
      }
    }

    override def postStop = {
      super.postStop
    }
  }
}

//class FetchService
//  ( val channel    : ChannelAddress,
//    val clientId   : String,
//    val offsets    : NodeOffsets)
//  extends FetchService.Actor with PoolDirectSinkChannel {
//
//  def path: String = "/user/push-service/cluster/broker-service/" + channel.nodeId
//}

class FetchService
( val channel    : ChannelAddress,
  val clientId   : String,
  val offsets    : NodeOffsets)
  extends FetchService.Actor with KafkaService {

  val remote = new InetSocketAddress(channel.host, channel.port)
}

