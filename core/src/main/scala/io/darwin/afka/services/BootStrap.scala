package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{ActorRef, FSM, Props, Terminated}
import io.darwin.afka.packets.requests.{FetchRequest, MetaDataRequest}
import io.darwin.afka.packets.responses.{Broker, MetaDataResponse}

import scala.concurrent.duration._

/**
  * Created by darwin on 26/12/2016.
  */

object BootStrap {

  def props( remote     : InetSocketAddress,
             clientId   : String,
             listener   : ActorRef ) = {
    Props(classOf[BootStrap], remote, clientId, listener)
  }

  sealed trait State
  case object DISCONNECT   extends State
  case object CONNECTING   extends State
  case object BOOTSTRAP    extends State

  sealed trait Data
  case object Dummy extends Data
  case object Fetch extends Data

  trait Actor extends FSM[State, Data]  {
    this: Actor with KafkaService {
      val listener: ActorRef
    } ⇒

    startWith(CONNECTING, Dummy)

    when(DISCONNECT, stateTimeout = 1 second) {
      case Event(StateTimeout, _) ⇒ {
        reconnect
        goto(CONNECTING)
      }
    }

    when(CONNECTING, stateTimeout = 1 second) {
      case Event(KafkaClientConnected(_), _) ⇒ {
        send(MetaDataRequest())
        goto(BOOTSTRAP)
      }
    }

    when(BOOTSTRAP, stateTimeout = 5 second) {
      case Event(meta: MetaDataResponse, _) ⇒ {
        onMetaResponse(meta)
      }
      case Event(StateTimeout, Fetch) ⇒ {
        send(MetaDataRequest())
        stay using(Dummy)
      }
    }

    private def onMetaResponse(meta: MetaDataResponse) = {
      def logging = {
        log.info(s"cluster: ${meta.clusterId.getOrElse("")}, controller: ${meta.controllerId}")
        meta.brokers.foreach {
          case Broker(node, host, port, rack) ⇒
            log.info(s"broker = { node: ${node}, host: ${host}, port: ${port} }")
        }
      }

      logging

      // controller id = -1 means the meta data isn't right, re-fetch!
      if(meta.controllerId < 0) {
        stay using(Fetch)
      }
      else{
        listener ! meta
        stop
      }
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

class BootStrap
  ( val remote   : InetSocketAddress,
    val clientId : String,
    val listener : ActorRef )
  extends KafkaActor with BootStrap.Actor with KafkaService
