package io.darwin.afka.services.common

import java.net.InetSocketAddress

import akka.actor.{ActorLogging, ActorRef, Terminated}
import akka.io.Tcp.Write
import akka.util.ByteString
import io.darwin.afka.decoder.{KafkaDecoder, decode}
import io.darwin.afka.encoder.encode
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses._
import io.darwin.afka.byteOrder

import scala.collection.mutable.Map

case class RequestPacket(request: KafkaRequest, who: ActorRef)
case class ResponsePacket(response: Any, req: RequestPacket)
case class InternalResp(rsp: ResponsePacket, reply: ActorRef)

/**
  * Created by darwin on 27/12/2016.
  */

trait KafkaService extends KafkaActor with KafkaServiceSinkChannel with ActorLogging {
  this: {
    val remote: InetSocketAddress
    val clientId: String
  } ⇒

  private var client: Option[ActorRef] = None

  protected def reconnect = {
    closeConnection
    client = Some(context.actorOf(KafkaNetworkClient.props(remote = remote, owner = self), "client"))
    context watch client.get
  }

  override def preStart = {
    super.preStart
    reconnect
  }

  private var lastCorrelationId: Int = 0
  protected def suicide = context stop self

  private var socket: Option[ActorRef] = None

  private var pendingRequests: Map[Int,  (RequestPacket, ActorRef)] = Map.empty

  protected def doSend[A <: KafkaRequest](req: A) = {
    lastCorrelationId += 1
    socket.get ! Write(encode(req, lastCorrelationId, clientId))
  }

  protected def send(request: RequestPacket, from: ActorRef) = {
    doSend(request.request)
    pendingRequests += lastCorrelationId → (request, from)
  }

  override def sending[A <: KafkaRequest](req: A, from: ActorRef) = {
    send(RequestPacket(req, from), from)
  }

  private def decodeResponseBody(request: RequestPacket, data: ByteString, from: ActorRef): Unit = {
    def decodeRsp[A](data: ByteString)(implicit decoder: KafkaDecoder[A]) = {
      super.receive(InternalResp(ResponsePacket(decode[A](data), request), from))
    }

    val apiKey = request.request.apiKey
    if(apiKey == GroupCoordinateRequest.apiKey)   decodeRsp[GroupCoordinateResponse](data)
    else if(apiKey == MetaDataRequest.apiKey)     decodeRsp[MetaDataResponse](data)
    else if(apiKey == HeartBeatRequest.apiKey)    decodeRsp[HeartBeatResponse](data)
    else if(apiKey == JoinGroupRequest.apiKey)    decodeRsp[JoinGroupResponse](data)
    else if(apiKey == SyncGroupRequest.apiKey)    decodeRsp[SyncGroupResponse](data)
    else if(apiKey == OffsetFetchRequest.apiKey)  decodeRsp[OffsetFetchResponse](data)
    else if(apiKey == FetchRequest.apiKey)        decodeRsp[FetchResponse](data)
    else if(apiKey == OffsetCommitRequest.apiKey) decodeRsp[OffsetCommitResponse](data)
    else {
      log.warning(s"unknown event ${apiKey} received")
      super.receive(data)
    }
  }

  private def decodeResponse(data: ByteString) = {
    val id = data.iterator.getInt
    val req = pendingRequests.get(id)

    if(req.isEmpty) {
      log.error(s"the received correlation id ${id} != ${lastCorrelationId}")
      suicide
    }
    else {
      pendingRequests -= id
      val r = req.get
      decodeResponseBody(r._1, data.slice(4, data.length), r._2)
    }
  }

  private def clientDead = {
    client = None
    socket = None
  }

  override def receive: Receive = {
    case c @ KafkaClientConnected(conn: ActorRef) ⇒ {
      socket = Some(conn)
      super.receive(c)
      context become {
        case KafkaResponseData(data: ByteString) ⇒ decodeResponse(data)
        case t@Terminated(_) ⇒ {
          clientDead
          super.receive(t)
        }
        case e ⇒ super.receive(e)
      }
    }
    case e ⇒ super.receive(e)
  }

  def closeConnection = {
    client.map(context.stop(_))
    clientDead
  }
}
