package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{ActorRef, ActorSelection, FSM, Props, Terminated}
import io.darwin.afka.TopicId
import io.darwin.afka.packets.responses.MetaDataResponse
import io.darwin.afka.services.BrokerMaster.BrokerRequest

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
  case class ClusterReady()
}


import io.darwin.afka.services.ClusterService._

class ClusterService(val clusterId  : String,
                     val bootstraps : Array[InetSocketAddress],
                     val listener   : ActorRef)
  extends FSM[State, Data]
{
  var workers = bootstraps.map { host ⇒
    context.actorOf(BootStrap.props
    ( remote   = host,
      clientId = "cluster",
      listener = self))
  }

  startWith(BOOTSTRAP, Dummy)

  when(BOOTSTRAP) {
    case Event(meta: MetaDataResponse, Dummy) ⇒ {
      onMetaReceived(meta)
    }
  }

  when(READY) {
    case Event(e: MetaDataResponse, Dummy) ⇒ stay
  }

  var brokers: Option[ActorRef] = None

  def onMetaReceived(meta: MetaDataResponse) = {
    log.info("meta data")
    workers.foreach(context.stop)

    brokers = Some(context.actorOf(BrokerService.props(
          brokers  = meta.brokers,
          clientId = "push-service",
          listener = self),
      "broker-service"))

    listener ! ClusterReady()

    goto(READY)
  }

  whenUnhandled {
    case Event(e: CreateConsumer, _) ⇒ {
      if (brokers.isDefined) {
        log.info("create consumer")
        brokers.get.forward(BrokerRequest(e))
      } else {
        log.warning("not ready yet")
      }
      stay
    }
  }
}


