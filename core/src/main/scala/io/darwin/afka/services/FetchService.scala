package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{ActorLogging, Props}
import akka.util.ByteString
import io.darwin.afka.PartitionId
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses._
import io.darwin.afka.decoder.decoding
import io.darwin.afka.domain.FetchedMessages
import io.darwin.afka.packets.common.ProtoMessageInfo

import scala.collection.mutable.MutableList


/**
  * Created by darwin on 30/12/2016.
  */
object FetchService {

  def props( remote     : InetSocketAddress,
             clientId   : String,
             request    : FetchRequest ) = {
    Props(classOf[FetchService], remote, clientId, request)
  }



  trait Actor extends KafkaActor with ActorLogging {
    this: Actor with KafkaService {
      val request    : FetchRequest
    } ⇒

    request.topics.foreach {
      case FetchTopicRequest(topic, parts) ⇒
        log.info(s"topic = ${topic}, parts = ${parts.map( r ⇒ s"part=${r.partition} offset=${r.offset}").mkString("\n")}")
    }

    override def receive: Receive = {
      case KafkaClientConnected(_)       ⇒ onConnected
      case msg    : FetchResponse        ⇒ onMessageFetched(msg)
    }

    private def onConnected = {
      send(request)
    }

    private def onMessageFetched(msg: FetchResponse) = {
      log.info(s"fetch response received: topics=${msg.topics.length}")

      FetchedMessages.decode(1, msg)
    }
  }
}

class FetchService
  ( val remote     : InetSocketAddress,
    val clientId   : String,
    val request    : FetchRequest)
  extends KafkaActor with FetchService.Actor with KafkaService
