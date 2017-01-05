package io.darwin.afka.services.domain

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.pattern._
import akka.util.Timeout
import io.darwin.afka.TopicId
import io.darwin.afka.domain.KafkaCluster
import io.darwin.afka.packets.requests.{GroupCoordinateRequest, MetaDataRequest}
import io.darwin.afka.packets.responses._
import io.darwin.afka.services.common.ResponsePacket
import io.darwin.afka.services.pool.ClusterService.ClusterChanged

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

  import ExecutionContext.Implicits.global

  private def onRequiredBrokerOffline = {
    log.error("SUBSCRIBE")
    context.system.eventStream.subscribe(self, classOf[ClusterChanged])
  }
  def hasOfflineLeader(topics: Array[TopicMetaData]): Boolean = {
    (topics.filter(_.errorCode != KafkaErrorCode.NO_ERROR).length > 0) ||
    topics.count { t ⇒
      t.partitions.count{ p ⇒ (p.errorCode != 0) || p.leader < 0} > 0
    } > 0
  }

  def onCoordinatorReceived(meta: MetaDataResponse, c: Coordinator) = {
    if (hasOfflineLeader(meta.topics)) {
      onRequiredBrokerOffline
    }

    coordinator = Some(context.actorOf(GroupCoordinator.props(
      coordinator = c,
      groupId = group,
      cluster = KafkaCluster(meta),
      topics = topics),
      "coordinator"))

    coordinator.foreach(context watch)
  }

  private def onMetaDataReceived(meta: MetaDataResponse): Unit = {
    implicit val timeout: Timeout = Timeout(1 second)
    (cluster ? GroupCoordinateRequest(group)) onComplete {
      case Success(ResponsePacket(c: GroupCoordinateResponse, _)) ⇒
        c.error match{
          case KafkaErrorCode.NO_ERROR ⇒ onCoordinatorReceived(meta, c.coordinator)
          case KafkaErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE ⇒ onRequiredBrokerOffline
          case _ ⇒ {
            log.warning(s"@GroupCoordinateResponse: ${c.error}")
            context.system.scheduler.scheduleOnce(5 second)(onMetaDataReceived(meta))
          }
        }
      case _ ⇒ context stop self
    }
  }

  def startup = {
    implicit val timeout: Timeout = Timeout(10 second)
    (cluster ? MetaDataRequest(Some(topics))) onComplete {
      case Success(meta: MetaDataResponse) ⇒ onMetaDataReceived(meta)
      case _                               ⇒ context stop self
    }
  }

  context.system.scheduler.scheduleOnce(1 second)(startup)

  override def receive = {
    case Terminated(_)    ⇒ context stop self
    case _:ClusterChanged ⇒
      log.error("CLUSTER CHANGED")
      context stop self
    case e                ⇒ log.info(s"${e}")
  }

  override def postStop(): Unit = {
    context.system.eventStream.unsubscribe(self, classOf[ClusterChanged])
    super.postStop()
  }
}
