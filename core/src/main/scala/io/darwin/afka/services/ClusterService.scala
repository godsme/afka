package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.FSM.Event
import akka.actor.{ActorRef, FSM, Props}
import io.darwin.afka.packets.responses.MetaDataResponse

/**
  * Created by darwin on 1/1/2017.
  */
object ClusterService {

  def props( clusterId  : String,
             bootstraps : Array[InetSocketAddress]) = {
    Props(classOf[ClusterService], clusterId, bootstraps)
  }

  sealed trait State
  case object BOOTSTRAP extends State
  case object READY     extends State

  sealed trait Data
  case object Dummy extends Data
}


import ClusterService._

class ClusterService(val clusterId  : String,
                     val bootstraps : Array[InetSocketAddress])
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
    case _ ⇒ stay
  }

  def onMetaReceived(meta: MetaDataResponse) = {
    log.info("meta data")
    workers.foreach(context.stop)

    goto(READY)
  }
}


