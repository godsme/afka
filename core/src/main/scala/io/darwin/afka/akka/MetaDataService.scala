package io.darwin.afka.akka

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, Props}
import akka.util.ByteString
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses.{GroupCoordinateResponse, JoinGroupResponse, MetaDataResponse}
import io.darwin.afka.decoder.decoding
import io.darwin.afka.encoder.encoding
import io.darwin.afka.domain.KafkaCluster
import io.darwin.afka.packets.common.ConsumerGroupMeta


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

  def sendCoordinatorRequest = {
    send(GroupCoordinateRequest("my-group"))
  }

  private def decodeMetadataRsp(data: ByteString) = {
    val meta = decoding[MetaDataResponse](ByteStringSourceChannel(data))
    cluster = Some(KafkaCluster(meta))
    log.info(s"packet: ${data.size} received\n ${cluster.get}")

    sendCoordinatorRequest
  }

  private def decodeCoordinatorRsp(data: ByteString) = {
    val co = decoding[GroupCoordinateResponse](ByteStringSourceChannel(data))
    log.info(s"error = ${co.error}, nodeid=${co.coordinator.nodeId}, host=${co.coordinator.host}, port=${co.coordinator.port}")
    joinGroup
  }

  private def joinGroup = {
    val groupMeta = ByteStringSinkChannel().encodeWithoutSize(ConsumerGroupMeta(subscription = Array("my-topic", "darwin")))
    send(JoinGroupRequest(groupId="my-group", protocols=Array(GroupProtocol(meta=groupMeta))))
  }

  private def decodeJoinGroupRsp(data: ByteString) = {
    val rsp = decoding[JoinGroupResponse](ByteStringSourceChannel(data))
    log.info(s"error=${rsp.errorCode}, generated-id=${rsp.generatedId}, proto=${rsp.groupProtocol}, leader=${rsp.leaderId}, member=${rsp.memberId}, members=${rsp.members.mkString(",")}")
  }

  override def decodeResponseBody(data: ByteString) = {
    if(lastApiKey == MetaDataRequest.apiKey) {
      decodeMetadataRsp(data)
    }
    else if(lastApiKey == GroupCoordinateRequest.apiKey) {
      decodeCoordinatorRsp(data)
    }
    else if(lastApiKey == JoinGroupRequest.apiKey) {
      decodeJoinGroupRsp(data)
    }
  }
}
