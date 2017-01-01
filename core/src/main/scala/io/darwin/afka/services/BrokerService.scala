package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.routing.{ActorRefRoutee, RandomRoutingLogic, RoundRobinRoutingLogic, Router}
import io.darwin.afka.packets.responses.BrokerResponse
import io.darwin.afka.services.BrokerMaster.BrokerRequest

/**
  * Created by darwin on 2/1/2017.
  */
object BrokerService {
  def props( brokers: Array[BrokerResponse],
             clientId   : String,
             listener   : ActorRef ) = {
    Props(classOf[BrokerService], brokers, clientId, listener)
  }
}

class BrokerService( val brokers: Array[BrokerResponse] ,
                     val clientId : String,
                     val listener : ActorRef)
  extends Actor with ActorLogging {

  def createBroker(i: Int) = {
    val BrokerResponse(node, host, port, _) = brokers(i)
    val r = context.actorOf(BrokerMaster.props(
      remote   = new InetSocketAddress(host, port),
      clientId = "push-service",
      node     = node,
      listener = self), s"broker-${node}")

    context watch r
    r
  }

  var router = {
    val routees = for(i ← 0 until brokers.length) yield {
      val r = createBroker(i)
      ActorRefRoutee(r)
    }

    Router(RandomRoutingLogic(), routees)
  }

  def receive = {
    case r: BrokerRequest =>
      router.route(r, sender())
    case Terminated(a) =>
      router = router.removeRoutee(a)
      val r = createBroker(a.path.name.toInt)
      router = router.addRoutee(r)
  }
}