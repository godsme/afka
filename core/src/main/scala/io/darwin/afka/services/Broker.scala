package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{ActorRef, FSM, Props, Terminated}
import io.darwin.afka.packets.requests.KafkaRequest

import scala.concurrent.duration._

/**
  * Created by darwin on 2/1/2017.
  */
object Broker {

  def props( remote     : InetSocketAddress,
             clientId   : String,
             listener   : ActorRef ) = {
    Props(classOf[Broker], remote, clientId, listener)
  }


  sealed trait State
  case object DISCONNECT   extends State
  case object CONNECTING   extends State
  case object CONNECTED    extends State

  sealed trait Data
  case object Dummy extends Data

  trait Actor extends FSM[State, Data] {
    this: Actor with KafkaService {
      val listener: ActorRef
    } ⇒

    startWith(CONNECTING, Dummy)

    when(DISCONNECT, stateTimeout = 5 second) {
      case Event(StateTimeout, _) ⇒ {
        reconnect
        goto(CONNECTING)
      }
    }

    when(CONNECTING, stateTimeout = 5 second) {
      case Event(KafkaClientConnected(_), _) ⇒ {
        listener ! WorkerReady(self)
        goto(CONNECTED)
      }
    }

    when(CONNECTED) {
      case Event(RoutingEvent(request: RequestPacket), _) ⇒ handleRequest(request)
      case Event(r@ResponsePacket(_, req: RequestPacket), _) ⇒ {
        req.who ! r
        stay
      }
    }

    def handleRequest(request: RequestPacket) = {
      val RequestPacket(req: KafkaRequest, who: ActorRef) = request
      send(req, who)
      stay
    }

    whenUnhandled {
      case Event(Terminated(_), _) |
           Event(StateTimeout, _) ⇒ {
        closeConnection
        goto(DISCONNECT)
      }
    }
  }
}

class Broker
  ( val remote   : InetSocketAddress,
    val clientId : String,
    val listener : ActorRef )
  extends KafkaActor with Broker.Actor with KafkaService


