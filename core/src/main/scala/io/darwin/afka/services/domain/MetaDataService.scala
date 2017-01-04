package io.darwin.afka.services.domain

import akka.actor.FSM.Failure
import akka.actor.{ActorRef, FSM, Props, Terminated}
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses.{Coordinator, GroupCoordinateResponse, KafkaErrorCode, MetaDataResponse}
import io.darwin.afka.services.common.{ChannelConnected, KafkaActor, KafkaService, KafkaServiceSinkChannel}
import io.darwin.afka.services.pool.PoolSinkChannel

import scala.concurrent.duration._

/**
  * Created by darwin on 26/12/2016.
  */

object MetaDataService {

  def props( clientId  : String,
             groupId   : String,
             topics    : Array[String],
             listener  : ActorRef) = {
    Props(classOf[MetaDataService], clientId, groupId, topics, listener)
  }

  case class SessionMetaData(meta: MetaDataResponse, coordinator: Coordinator)

  sealed trait State
  case object CONNECTING  extends State
  case object PHASE1      extends State
  case object PHASE2      extends State

  sealed trait Data
  case object Dummy extends Data

  ///////////////////////////////////////////////////////////////
  trait Actor extends FSM[State, Data] {
    this: KafkaActor with KafkaServiceSinkChannel {
      val groupId  : String
      val topics   : Array[String]
      val listener : ActorRef
    } ⇒

    private var metaData: Option[MetaDataResponse] = None

    startWith(CONNECTING, Dummy)

    when(CONNECTING) {
      case Event(ChannelConnected, _) ⇒ {
        sending(MetaDataRequest(Some(topics)))
        goto(PHASE1)
      }
    }

    when(PHASE1) {
      case Event(r: MetaDataResponse, _) ⇒ {
        onMetadataRsp(r)
        goto(PHASE2)
      }
    }

    when(PHASE2, 2 second) {
      case Event(StateTimeout, _) ⇒ {
        sending(GroupCoordinateRequest(groupId))
        stay
      }
      case Event(r: GroupCoordinateResponse, _) ⇒ onCoordinatorRsp(r)
    }

//    override def receive: Receive = {
//      case    ChannelConnected        ⇒ sending(MetaDataRequest(Some(topics)))
//      case r: MetaDataResponse        ⇒ onMetadataRsp(r)
//      case r: GroupCoordinateResponse ⇒ onCoordinatorRsp(r)
//      case r: Terminated ⇒ {
//        log.error(s"actor terminated ${r.actor}")
//        context stop self
//      }
//    }

    private def onMetadataRsp(meta: MetaDataResponse) = {
      metaData = Some(meta)
      sending(GroupCoordinateRequest(groupId))
    }

    private def onCoordinatorRsp(co: GroupCoordinateResponse) = {
      log.info(
        s"error  = ${co.error}, "              +
        s"nodeId = ${co.coordinator.nodeId}, " +
        s"host   = ${co.coordinator.host}, "   +
        s"port   = ${co.coordinator.port}")

      co.error match {
        case KafkaErrorCode.NO_ERROR ⇒
          listener ! SessionMetaData(metaData.get, co.coordinator)
          stop
        case KafkaErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE ⇒
          log.warning(s"Group ${groupId} coordinator not available.")
          // start a timer to restart
          stay
        case e ⇒
          stop(Failure(e))
      }

    }

    initialize()
  }
}

//          context.actorOf( GroupCoordinator.props(
//            coordinator   = co.coordinator,
//            clientId = clientId,
//            groupId  = groupId,
//            cluster  = cluster.get,
//            topics   = topics))

//class MetaDataService
//   ( val remote   : InetSocketAddress,
//     val clientId : String,
//     val groupId  : String,
//     val topics   : Array[String],
//     val listener : ActorRef)
//  extends MetaDataService.Actor with KafkaService

class MetaDataService
   ( val clientId : String,
     val groupId  : String,
     val topics   : Array[String],
     val listener : ActorRef)
  extends KafkaActor with MetaDataService.Actor with PoolSinkChannel {

  def path: String = "/user/push-service/cluster"
}