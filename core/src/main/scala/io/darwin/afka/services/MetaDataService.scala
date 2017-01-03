package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{ActorLogging, ActorRef, Props, Terminated}
import io.darwin.afka.domain.KafkaCluster
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses.{GroupCoordinateResponse, KafkaErrorCode, MetaDataResponse}

/**
  * Created by darwin on 26/12/2016.
  */

object MetaDataService {

  def props( remote    : InetSocketAddress,
             clientId  : String,
             groupId   : String,
             topics    : Array[String],
             listener  : ActorRef = null) = {
    Props(classOf[MetaDataService], remote, clientId, groupId, topics, listener)
  }

  ///////////////////////////////////////////////////////////////
  trait Actor extends KafkaActor with ActorLogging {
    this: Actor with KafkaService {
      val clientId : String
      val groupId  : String
      val topics   : Array[String]
    } ⇒

    private var cluster: Option[KafkaCluster] = None

    override def receive: Receive = {
      case KafkaClientConnected(_)        ⇒ sending(MetaDataRequest(Some(topics)))
      case meta:  MetaDataResponse        ⇒ handleMetadataRsp(meta)
      case coord: GroupCoordinateResponse ⇒ handleCoordinatorRsp(coord)
      case term: Terminated ⇒ {
        log.error(s"actor terminated ${term.actor}")
        context stop self
      }
    }

    private def handleMetadataRsp(meta: MetaDataResponse) = {
      cluster = Some(KafkaCluster(meta))
      log.info(s"\n${cluster.get}")

      sending(GroupCoordinateRequest(groupId))
    }

    private def handleCoordinatorRsp(co: GroupCoordinateResponse) = {
      log.info(
        s"error  = ${co.error}, "              +
        s"nodeId = ${co.coordinator.nodeId}, " +
        s"host   = ${co.coordinator.host}, "   +
        s"port   = ${co.coordinator.port}")

      co.error match {
        case KafkaErrorCode.NO_ERROR ⇒
          context.actorOf( GroupCoordinator.
            props( remote   = new InetSocketAddress(co.coordinator.host, co.coordinator.port),
              clientId = clientId,
              groupId  = groupId,
              cluster  = cluster.get,
              topics   = topics))

        case KafkaErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE ⇒
          log.warning(s"Group ${groupId} coordinator not available.")
          // start a timer to restart
        case KafkaErrorCode.GROUP_AUTHORIZATION_FAILED ⇒
          log.warning("Group authorization failed.")
      }
    }
  }
}

class MetaDataService
   ( val remote   : InetSocketAddress,
     val clientId : String,
     val groupId  : String,
     val topics   : Array[String],
     val listener : ActorRef)
  extends MetaDataService.Actor with KafkaService


