package io.darwin.afka.services.common

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Terminated}
import akka.contrib.pattern.ReceivePipeline
import akka.contrib.pattern.ReceivePipeline.{Delegation, HandledCompletely, Inner}
import akka.io.Tcp.Write
import akka.util.ByteString
import io.darwin.afka.decoder.{KafkaDecoder, decode}
import io.darwin.afka.encoder.encode
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses._
import io.darwin.afka.byteOrder

import scala.collection.mutable.MutableList

//case class ResponsePacket(response: Any, who: ActorRef)

/**
  * Created by darwin on 27/12/2016.
  */

trait KafkaService extends KafkaServiceSinkChannel with ReceivePipeline {
  this: Actor with ActorLogging {
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

  //private var pendingRequests: Map[Int,  (KafkaRequest, ActorRef)] = Map.empty
  private var pendingRequests: MutableList[(Int, (KafkaRequest, ActorRef))] = MutableList.empty

  protected def doSend[A <: KafkaRequest](req: A) = {
    if(socket.isDefined) {
      lastCorrelationId += 1
      socket.get ! Write(encode(req, lastCorrelationId, clientId))
    }
    else {
      log.error(s"try to send event to a closed socket ${req}")
    }
  }

  private def send(request: KafkaRequest, from: ActorRef = self) = {
    doSend(request)
    pendingRequests.+=( (lastCorrelationId, (request, from)) )
  }

  def sending[A <: KafkaRequest](req: A, from: ActorRef = self): Unit = {
    send(req, from)
  }

  private def decodeResponseBody(request: KafkaRequest, data: ByteString, from: ActorRef): Delegation = {
    def convert[A](o: A) = {
      if(self == from) Inner(o)
      else {
        from ! o
        HandledCompletely
      }
    }

    def decodeRsp[A](data: ByteString)(implicit decoder: KafkaDecoder[A]) = {
      convert(decode[A](data))
    }

    val apiKey = request.apiKey
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
      Inner(data)
    }
  }

  private def onCorrelationReceive(id: Int): Option[(KafkaRequest, ActorRef)] = {
    pendingRequests = pendingRequests.dropWhile(_._1 != id)

    if(pendingRequests.isEmpty) None
    else {
      Some(pendingRequests.head._2)
    }
  }
  private def decodeResponse(data: ByteString): Delegation = {
    val id = data.iterator.getInt
    val req = onCorrelationReceive(id)

    if(req.isEmpty) {
      log.error(s"the received correlation id ${id} cannot be found. ${lastCorrelationId}")
      suicide
      HandledCompletely
    }
    else {
      pendingRequests = pendingRequests.tail

      if(pendingRequests.size > 0)
      log.info(s"# of pending request = ${pendingRequests.size}, ${req.get._1.apiKey} ${req.get._2} ${id}~${lastCorrelationId}")
      val r = req.get
      decodeResponseBody(r._1, data.slice(4, data.length), r._2)
    }
  }

  private def clientDead = {
    client = None
    socket = None
  }

  pipelineOuter {
    case KafkaClientConnected(conn: ActorRef) ⇒ {
      socket = Some(conn)
      Inner(ChannelConnected(client.get))
    }
    case KafkaResponseData(data: ByteString)     ⇒ decodeResponse(data)
    case t@Terminated(who) ⇒ {
      if(client.fold(false)(_ == who)) {
        clientDead
      }
      Inner(t)
    }
  }

  def closeConnection = {
    client.foreach { c ⇒
      context unwatch c
      context stop c
    }

    clientDead
  }
}
