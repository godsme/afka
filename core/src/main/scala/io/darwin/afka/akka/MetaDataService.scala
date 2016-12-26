package io.darwin.afka.akka

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.io.{IO, Tcp}
import akka.util.{ByteString}
import io.darwin.afka.packets.requests.MetaDataRequest
import io.darwin.afka.packets.responses.MetaDataResponse
import io.darwin.afka.decoder.decoding

/**
  * Created by darwin on 26/12/2016.
  */

object MetaDataService {
  def props(remote: InetSocketAddress, listener: ActorRef = null) =
    Props(classOf[MetaDataService], remote, listener)
}

class MetaDataService( bootstrap: InetSocketAddress,
                       listener: ActorRef)
  extends Actor with ActorLogging {

  import Tcp._
 // import context.system

  val client: ActorRef = context.actorOf(KafkaNetworkClient.props(remote = bootstrap, owner = self), "client")

  log.info(s"client = ${client}")

  private def metaDataFetch(conn: ActorRef, topics: Option[Array[String]] = None) = {
    val buf = ByteStringSinkChannel()
    MetaDataRequest(topics).encode(buf, 5, "meta-data")
    conn ! Write(buf.get)
  }

  private def decodeRsp(data: ByteString) = {
    val buf = ByteStringSourceChannel(data)
    println(s"data = ${data.size} coid = ${buf.getInt}")
    val rsp = decoding[MetaDataResponse](buf)
    log.info(s"packet: ${data.size} - ${rsp.toString} received")

    listener ! rsp

    context.stop(self)
  }

  override def receive: Receive = {
    case KafkaClientConnected(conn: ActorRef) => {
      val connection = conn
      metaDataFetch(conn)
    }
    case KafkaResponseData(data: ByteString) => {
      decodeRsp(data)
    }
  }
}
