package io.darwin.afka.akka

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.io.Tcp.Write
import akka.util.ByteString
import io.darwin.afka.packets.requests.KafkaRequest

/**
  * Created by darwin on 27/12/2016.
  */
abstract class KafkaService(remote: InetSocketAddress, protected val clientId: String)
  extends Actor with ActorLogging {

  private var client: ActorRef = null
  private var socket: Option[ActorRef] = None

  override def preStart = {
    client = context.actorOf(KafkaNetworkClient.props(remote = remote, owner = self), "client")
    context watch client
    //log.info(s"client = ${client}")
    preStartMore()
  }

  private var lastCorrelationId: Int = 0
  protected var lastApiKey = 0

  protected def send(req: KafkaRequest) = {
    lastCorrelationId += 1
    lastApiKey = req.apiKey
    socket.get ! Write(ByteStringSinkChannel().encode(req, lastCorrelationId, clientId))
  }

  private def decodeResponse(data: ByteString) = {
    val id = data.iterator.getInt

    if(lastCorrelationId != id) {
      log.error(s"the received correlation id ${id} != ${lastCorrelationId}")
      context stop self
    } else {
      decodeResponseBody(data.slice(4, data.length))
    }
  }

  override def receive: Receive = {
    case KafkaClientConnected(conn: ActorRef) => {
      socket = Some(conn)
      onConnected(conn)
    }
    case KafkaResponseData(data: ByteString) => {
      decodeResponse(data)
    }
  }

  protected def preStartMore() = {}
  protected def onConnected(conn: ActorRef)
  protected def decodeResponseBody(data: ByteString)
}
