package io.darwin.afka.services.domain

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.pattern._
import akka.util.Timeout
import io.darwin.afka.TopicId
import io.darwin.afka.domain.KafkaCluster
import io.darwin.afka.packets.requests.{GroupCoordinateRequest, MetaDataRequest}
import io.darwin.afka.packets.responses.{Coordinator, GroupCoordinateResponse, MetaDataResponse}
import io.darwin.afka.services.common.ResponsePacket

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Success

/**
  * Created by darwin on 2/1/2017.
  */
object Consumer {
  def props( cluster: ActorRef,
             group  : String,
             topics : Array[TopicId]) =
    Props(classOf[Consumer], cluster, group, topics)
}

class Consumer
  ( val cluster : ActorRef,
    val group   : String,
    val topics  : Array[TopicId])
  extends Actor with ActorLogging {

  private var coordinator: Option[ActorRef] = None

  implicit val timeout: Timeout = Timeout(10 second)
  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global

  def onCoordinatorReceived(meta: MetaDataResponse, c: Coordinator) = {
    coordinator = Some(context.actorOf(GroupCoordinator.props(
        coordinator   = c,
        groupId       = group,
        cluster       = KafkaCluster(meta),
        topics        = topics),
      "coordinator"))
    context watch coordinator.get
  }

  def onMetaDataReceived(meta: MetaDataResponse) = {
    (cluster ? GroupCoordinateRequest(group)) onComplete {
      case Success(ResponsePacket(c: GroupCoordinateResponse, _)) ⇒ onCoordinatorReceived(meta, c.coordinator)
      case _                                                      ⇒ context stop self
    }
  }

  context.system.scheduler.scheduleOnce(1 second) {
    (cluster ? MetaDataRequest(Some(topics))) onComplete {
      case Success(meta: MetaDataResponse) ⇒ onMetaDataReceived(meta)
      case _                               ⇒ context stop self
    }
  }

  override def receive = {
    case Terminated(_) ⇒ context stop self
    case e             ⇒ log.info(s"${e}")
  }
}
