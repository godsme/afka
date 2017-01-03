package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{ActorRef, FSM, Props}
import io.darwin.afka.TopicId
import io.darwin.afka.packets.responses.MetaDataResponse

/**
  * Created by darwin on 1/1/2017.
  */
object ClusterService {

  def props( clusterId  : String,
             bootstraps : Array[InetSocketAddress],
             listener   : ActorRef) = {
    Props(classOf[ClusterService], clusterId, bootstraps, listener)
  }

  sealed trait State
  case object BOOTSTRAP extends State
  case object READY     extends State

  sealed trait Data
  case object Dummy extends Data

  case class CreateConsumer(groupId: String, topics: Array[TopicId])
  case object ClusterReady
}


import io.darwin.afka.services.ClusterService._

class ClusterService(val clusterId  : String,
                     val bootstraps : Array[InetSocketAddress],
                     val listener   : ActorRef)
  extends FSM[State, Data]
{
  var bootstrap = context.actorOf(BootStrapService.props(bootstraps, self))

  startWith(BOOTSTRAP, Dummy)

  when(BOOTSTRAP) {
    case Event(meta: MetaDataResponse, Dummy) ⇒ {
      onMetaReceived(meta)
    }
  }

  when(READY) {
    case Event(e: WorkerReady, Dummy) ⇒ {
      log.info(s"custer ${clusterId} is ready!")
      listener ! ClusterReady
      stay
    }
  }

  var brokers: Option[ActorRef] = None

  def onMetaReceived(meta: MetaDataResponse) = {
    context stop bootstrap

    brokers = Some(context.actorOf(BrokerService.props(
          brokers  = meta.brokers,
          clientId = "push-service",
          listener = self),
      "broker-service"))

    goto(READY)
  }

  whenUnhandled {
    case Event(e: CreateConsumer, _) ⇒ {
      if (brokers.isDefined) {
        brokers.get.forward(RoutingEvent(e))
      } else {
        log.warning("not ready yet")
      }
      stay
    }
  }
}


