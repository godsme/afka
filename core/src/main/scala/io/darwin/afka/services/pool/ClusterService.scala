package io.darwin.afka.services.pool

import java.net.InetSocketAddress

import akka.actor.{ActorRef, Cancellable, FSM, Props}
import io.darwin.afka.TopicId
import io.darwin.afka.domain.Brokers
import io.darwin.afka.packets.requests.{GroupCoordinateRequest, KafkaRequest, MetaDataRequest}
import io.darwin.afka.packets.responses.MetaDataResponse
import io.darwin.afka.services.common._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

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

  case class  CreateConsumer(groupId: String, topics: Array[TopicId])
  case object ClusterReady
  final case class  ClusterChanged()
}

import ClusterService._

class ClusterService(val clusterId  : String,
                     val bootstraps : Array[InetSocketAddress],
                     val listener   : ActorRef)
  extends FSM[State, Data]
{
  log.info("bootstrapping...")
  var bootstrap = context.actorOf(BootStrapService.props(bootstraps, self))
  var connection: Option[ActorRef] = None

  private def doSend[A <: KafkaRequest](req: A, who: ActorRef)(sending : (ActorRef, Any) ⇒ Unit): Boolean = {
    connection.fold(false) { c ⇒
      sending(c, RequestPacket(req, who))
      true
    }
  }

  private def send[A <: KafkaRequest](req: A, who: ActorRef = sender()): Boolean = {
    doSend(req, who)((c, m) ⇒ c ! m)
  }

  private def forward[A <: KafkaRequest](req: A, who: ActorRef = sender()): Boolean = {
    doSend(req, who)((c, m) ⇒ c.forward(m))
  }

  var brokers: Brokers = Brokers(Array.empty)

  startWith(BOOTSTRAP, Dummy)

  var metaFetcher: Option[Cancellable] = None

  when(BOOTSTRAP) {
    case Event(meta:MetaDataResponse, Dummy) ⇒ {
      onBootstrapped(meta)

      goto(READY)
    }
  }

  when(READY) {
    case Event(WorkerOnline, Dummy) ⇒ {
      log.info(s"custer ${clusterId} is ready!")
      listener ! ClusterReady

      if(metaFetcher.isEmpty) {
        import ExecutionContext.Implicits.global
        metaFetcher = Some(context.system.scheduler.schedule(0 milli, 5 second)(send(MetaDataRequest(), self)))
      }
      else{
        log.info("PUBLISH")
        try {
          context.system.eventStream.publish(ClusterChanged())
        }
        catch {
          case _: NoSuchElementException ⇒
            log.warning("no one subscribed this event")
          case e ⇒ throw e
        }
      }

      stay
    }
    case Event(e: MetaDataRequest, _) ⇒ {
      if(!send(e)) {
        sender ! NotReady(e)
      }
      stay
    }
    case Event(e: GroupCoordinateRequest, _) ⇒  {
      forward(e)
      stay
    }
    case Event(ResponsePacket(e: MetaDataResponse, req: RequestPacket), _) ⇒ {
      val newBrokers = Brokers(e.brokers)
      if(brokers != newBrokers) {
        log.info("new brokers are different")
        brokers = newBrokers
        connection.foreach(_ ! brokers)


      }

      if(req.who != self) {
        req.who ! e
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

  }

}


