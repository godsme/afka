package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{ActorRef, FSM, Props}
import io.darwin.afka.TopicId
import io.darwin.afka.domain.Brokers
import io.darwin.afka.packets.requests.{KafkaRequest, MetaDataRequest}
import io.darwin.afka.packets.responses.MetaDataResponse

import scala.collection.mutable.Map

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
  log.info("bootstrapping...")
  var bootstrap = context.actorOf(BootStrapService.props(bootstraps, self))
  var connection: Option[ActorRef] = None

  private def send[A <: KafkaRequest](req: A, who: ActorRef = self): Boolean = {
    connection.fold(false) { c ⇒
      c ! RoutingEvent(RequestPacket(req, who))
      true
    }
  }

  //val consumers: Map[String, (CreateConsumer, ActorRef)] = Map.empty
  var brokers: Brokers = Brokers(Array.empty)

  startWith(BOOTSTRAP, Dummy)

  when(BOOTSTRAP) {
    case Event(meta:MetaDataResponse, Dummy) ⇒ {
      onBootstrapped(meta)
    }
  }

  when(READY) {
    case Event(_: WorkerReady, Dummy) ⇒ {
      log.info(s"custer ${clusterId} is ready!")
      listener ! ClusterReady
      stay
    }
    case Event(e: CreateConsumer, _) ⇒ {
      if(!send(MetaDataRequest(Some(e.topics)))) {
        sender() ! NotReady(e)
      }
      stay
    }
    case Event(ResponsePacket(e: MetaDataResponse, req: RequestPacket), _) ⇒ {
      val newBrokers = Brokers(e.brokers)
      if(brokers != newBrokers) {
        log.info("new brokers are different")
        brokers = newBrokers
        connection.get ! brokers
      }

      stay
    }
  }

  def onBootstrapped(meta: MetaDataResponse) = {
    context stop bootstrap

    brokers = Brokers(meta.brokers)

    connection = Some(context.actorOf(BrokerService.props(
          brokers  = brokers,
          clientId = "push-service",
          listener = self),
      "broker-service"))

    goto(READY)
  }

}


