package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{ActorRef, FSM, Props, Terminated}
import akka.io.Tcp.ErrorClosed
import io.darwin.afka.packets.requests.KafkaRequest

import scala.concurrent.duration._

/**
  * Created by darwin on 2/1/2017.
  */
object BrokerConnection {

  def props( remote     : InetSocketAddress,
             clientId   : String,
             listener   : ActorRef ) = {
    Props(classOf[BrokerConnection], remote, clientId, listener)
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
        log.info("reconnect")
        reconnect
        goto(CONNECTING)
      }
    }

    when(CONNECTING, stateTimeout = 5 second) {
      case Event(KafkaClientConnected(_), _) ⇒ {
        listener ! WorkerOnline
        goto(CONNECTED)
      }
    }

    when(CONNECTED) {
      case Event(RoutingEvent(request: RequestPacket), _) ⇒ handleRequest(request)
      case Event(r@ResponsePacket(_, req: RequestPacket), _) ⇒ {
        req.who ! r
        stay
      }
      case Event(ErrorClosed(cause), _) ⇒ {
        listener ! WorkerOffline(cause)
        closeConnection
        goto(DISCONNECT)
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

class BrokerConnection
  ( val remote   : InetSocketAddress,
    val clientId : String,
    val listener : ActorRef )
  extends KafkaActor with BrokerConnection.Actor with KafkaService


