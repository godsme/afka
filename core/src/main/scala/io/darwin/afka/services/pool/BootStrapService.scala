package io.darwin.afka.services.pool

import java.net.InetSocketAddress

import akka.actor.{ActorRef, FSM, Props}
import io.darwin.afka.packets.requests.{KafkaRequest, MetaDataRequest}
import io.darwin.afka.packets.responses.MetaDataResponse
import io.darwin.afka.services.common.{ WorkerOnline}

import scala.concurrent.duration._

/**
  * Created by darwin on 3/1/2017.
  */
object BootStrapService {

  def props(bootstraps : Array[InetSocketAddress], listener: ActorRef) = {
    Props(classOf[BootStrapService], bootstraps, listener)
  }

  sealed trait State
  case object INIT       extends State
  case object BOOTSTRAP  extends State

  sealed trait Data
  case object Dummy extends Data
  case object Fetch extends Data
}

import io.darwin.afka.services.pool.BootStrapService._

class BootStrapService
  ( val bootstraps : Array[InetSocketAddress]
  , val listener   : ActorRef)
  extends FSM[State, Data] {

  var bootstrap = context.actorOf(BootstrapMaster.props(bootstraps, self))
  def send[A <: KafkaRequest](any: A) = bootstrap ! any

  startWith(INIT, Dummy)

  when(INIT) {
    case Event(WorkerOnline, _) ⇒ {
      send(MetaDataRequest())
      goto(BOOTSTRAP)
    }
  }

  when(BOOTSTRAP, stateTimeout = 1 second) {
    case Event(StateTimeout, _) ⇒ {
      send(MetaDataRequest())
      stay
    }
    case Event(r: MetaDataResponse, _) ⇒ {
      if(r.controllerId < 0) stay
      else {
        listener ! r
        stop
      }
    }
  }
}
