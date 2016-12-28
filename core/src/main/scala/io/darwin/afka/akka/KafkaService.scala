package io.darwin.afka.akka

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, FSM, Terminated}
import akka.io.Tcp.Write
import akka.util.ByteString
import io.darwin.afka.decoder.{KafkaDecoder, decoding}
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses._


/**
  * Created by darwin on 27/12/2016.
  */
trait KafkaActor extends Actor {
  override def receive: Receive = {case _ ⇒ throw new Exception("an actor should implemented receive")}
}

trait KafkaService extends KafkaActor with ActorLogging {
  this: {
    val remote: InetSocketAddress
    val clientId: String
  } ⇒

  private val client = context.actorOf(KafkaNetworkClient.props(remote = remote, owner = self), "client")
  context watch client

  private var lastCorrelationId: Int = 0
  protected var lastApiKey = 0
  protected def suicide = context stop self

  private var socket: Option[ActorRef] = None

  protected def send(req: KafkaRequest) = {
    lastCorrelationId += 1
    lastApiKey = req.apiKey
    socket.get ! Write(ByteStringSinkChannel().encode(req, lastCorrelationId, clientId))
  }

  private def decodeResponseBody(data: ByteString): Unit = {
    def decodeRsp[A](data: ByteString)(implicit decoder: KafkaDecoder[A]) = {
      super.receive(decoding[A](ByteStringSourceChannel(data)))
    }

    if(lastApiKey      == MetaDataRequest.apiKey) decodeRsp[MetaDataResponse](data)
    else if(lastApiKey == GroupCoordinateRequest.apiKey) decodeRsp[GroupCoordinateResponse](data)
    else if(lastApiKey == HeartBeatRequest.apiKey) decodeRsp[HeartBeatResponse](data)
    else if(lastApiKey == JoinGroupRequest.apiKey) decodeRsp[JoinGroupResponse](data)
    else if(lastApiKey == SyncGroupRequest.apiKey) decodeRsp[SyncGroupResponse](data)
    else {
      log.warning(s"unknown event ${lastApiKey} received")
      super.receive(data)
    }
  }

  private def decodeResponse(data: ByteString) = {
    val id = data.iterator.getInt

    if(lastCorrelationId != id) {
      log.error(s"the received correlation id ${id} != ${lastCorrelationId}")
      suicide
    } else {
      decodeResponseBody(data.slice(4, data.length))
    }
  }

  override def receive: Receive = {
    case c @ KafkaClientConnected(conn: ActorRef) ⇒ {
      socket = Some(conn)
      super.receive(c)
      context become {
        case KafkaResponseData(data: ByteString) ⇒ decodeResponse(data)
        case e ⇒ super.receive(e)
      }
    }
    case e ⇒ super.receive(e)
  }
}
