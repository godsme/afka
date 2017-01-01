package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{ActorRef, FSM, Props, Terminated}
import io.darwin.afka.TopicId
import io.darwin.afka.packets.requests.GroupCoordinateRequest
import io.darwin.afka.packets.responses.GroupCoordinateResponse
import io.darwin.afka.services.BrokerMaster.BrokerRequest
import io.darwin.afka.services.ClusterService.CreateConsumer

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

  case class BrokerConnected(who: ActorRef)

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
        listener ! BrokerConnected(self)
        goto(CONNECTED)
      }
    }

    when(CONNECTED) {
      case Event(request: BrokerRequest, _) ⇒ handleRequest(request)
      case Event(ResponsePacket(r:GroupCoordinateResponse, who), _) ⇒ {
        log.info("coord response received")
        who ! r
        stay
      }
    }

    var s: ActorRef = null

    def handleRequest(request: BrokerRequest) = {
      request.packet match {
        case CreateConsumer(groupId, topics) ⇒ {
          send(GroupCoordinateRequest(groupId), sender())
        }
      }
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


