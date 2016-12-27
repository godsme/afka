package io.darwin.afka.akka

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, Props}
import akka.util.ByteString
import io.darwin.afka.packets.requests.MetaDataRequest
import io.darwin.afka.packets.responses.MetaDataResponse
import io.darwin.afka.decoder.decoding
import io.darwin.afka.domain.KafkaCluster


/**
  * Created by darwin on 26/12/2016.
  */

object MetaDataService {
  def props(remote: InetSocketAddress, listener: ActorRef = null) =
    Props(classOf[MetaDataService], remote, listener)
}

class MetaDataService( bootstrap: InetSocketAddress,
                       listener: ActorRef)
  extends KafkaService(bootstrap, "cluster-meta-data") {


  private var cluster: Option[KafkaCluster] = None

  override def onConnected(conn: ActorRef) = {
    send(MetaDataRequest())
  }

  private def decodeMetadataRsp(data: ByteString) = {
    val meta = decoding[MetaDataResponse](ByteStringSourceChannel(data))
    cluster = Some(KafkaCluster(meta))
    log.info(s"packet: ${data.size} received\n ${cluster.get}")
  }

  override def decodeResponseBody(data: ByteString) = {
    if(lastApiKey == MetaDataRequest.apiKey) {
      decodeMetadataRsp(data)
    }

  }
}
