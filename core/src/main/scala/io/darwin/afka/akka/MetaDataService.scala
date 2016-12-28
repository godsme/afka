package io.darwin.afka.akka

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses.{GroupCoordinateResponse, JoinGroupResponse, MetaDataResponse}
import io.darwin.afka.domain.KafkaCluster
import io.darwin.afka.packets.common.ConsumerGroupMeta

/**
  * Created by darwin on 26/12/2016.
  */

object MetaDataService {
  def props(remote: InetSocketAddress, listener: ActorRef = null) =
    Props(classOf[MetaDataService], remote, listener)

  class MetaDataActor
    extends KafkaActor with ActorLogging {
    this: MetaDataActor with KafkaService ⇒

    private var cluster: Option[KafkaCluster] = None

    override def receive: Receive = {
      case KafkaClientConnected(_) ⇒ send(MetaDataRequest())
      case meta: MetaDataResponse  ⇒ handleMetadataRsp(meta)
      case coord: GroupCoordinateResponse ⇒ handleCoordinatorRsp(coord)
      case join: JoinGroupResponse ⇒ handleJoinRsp(join)
    }

    def sendCoordinatorRequest = {
      send(GroupCoordinateRequest("my-group"))
    }

    private def handleMetadataRsp(meta: MetaDataResponse) = {
      cluster = Some(KafkaCluster(meta))
      log.info(s"meta data response received\n ${cluster.get}")

      sendCoordinatorRequest
    }

    private def handleCoordinatorRsp(co: GroupCoordinateResponse) = {
      log.info(s"error = ${co.error}, nodeid=${co.coordinator.nodeId}, host=${co.coordinator.host}, port=${co.coordinator.port}")
      joinGroup
    }

    private def joinGroup = {
      val groupMeta = ByteStringSinkChannel().encodeWithoutSize(ConsumerGroupMeta(subscription = Array("my-topic", "darwin")))
      send(JoinGroupRequest(groupId="my-group", protocols=Array(GroupProtocol(meta=groupMeta))))
    }

    private def handleJoinRsp(rsp: JoinGroupResponse) = {
      log.info(s"error=${rsp.errorCode}, generated-id=${rsp.generatedId}, proto=${rsp.groupProtocol}, leader=${rsp.leaderId}, member=${rsp.memberId}, members=${rsp.members.mkString(",")}")
    }
  }
}

class MetaDataService(val remote: InetSocketAddress, val listener: ActorRef)
  extends MetaDataService.MetaDataActor with KafkaService {
  val clientId: String = "meta-data"
}


