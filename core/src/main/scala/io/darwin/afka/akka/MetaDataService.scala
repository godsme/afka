package io.darwin.afka.akka

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses.{GroupCoordinateResponse, JoinGroupResponse, MetaDataResponse}
import io.darwin.afka.domain.KafkaCluster
import io.darwin.afka.packets.common.ProtoSubscription

/**
  * Created by darwin on 26/12/2016.
  */

object MetaDataService {

  def props( remote: InetSocketAddress,
             clientId: String = "meta-data",
             listener: ActorRef = null) = {
    Props(classOf[MetaDataService], remote, clientId, listener)
  }


  class Actor
    extends KafkaActor with ActorLogging {
    this: Actor with KafkaService ⇒

    private var cluster: Option[KafkaCluster] = None

    override def receive: Receive = {
      case KafkaClientConnected(_) ⇒ {
        send(MetaDataRequest())
      }
      case meta:  MetaDataResponse  ⇒ handleMetadataRsp(meta)
      case coord: GroupCoordinateResponse ⇒ handleCoordinatorRsp(coord)
    }

    def sendCoordinatorRequest = {
      send(GroupCoordinateRequest("my-group"))
    }

    private def handleMetadataRsp(meta: MetaDataResponse) = {
      cluster = Some(KafkaCluster(meta))
      log.info(s"meta data response received\n${cluster.get}")

      sendCoordinatorRequest
    }

    private def handleCoordinatorRsp(co: GroupCoordinateResponse) = {
      log.info(s"error = ${co.error}, nodeid=${co.coordinator.nodeId}, host=${co.coordinator.host}, port=${co.coordinator.port}")
    }
  }
}

class MetaDataService
   ( val remote: InetSocketAddress
   , val clientId: String
   , val listener: ActorRef)
  extends MetaDataService.Actor with KafkaService


