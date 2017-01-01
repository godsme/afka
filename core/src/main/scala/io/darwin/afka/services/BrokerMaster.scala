package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}

/**
  * Created by darwin on 2/1/2017.
  */
object BrokerMaster {
  def props( remote     : InetSocketAddress,
             clientId   : String,
             node       : Int,
             listener   : ActorRef ) = {
    Props(classOf[BrokerMaster], remote, clientId, node, listener)
  }

  case class BrokerRequest(packet: Any)
}

import BrokerMaster._

class BrokerMaster(val remote   : InetSocketAddress,
                   val clientId : String,
                   val node     : Int,
                   val listener : ActorRef) extends Actor with ActorLogging {

  def createBroker(i: String) = {
    val r = context.actorOf(Broker.props(remote, clientId, self), i)
    context watch r
    r
  }

  var router = {
    val routees = for(i ← 0 until 5) yield {
      val r = createBroker(i.toString)
      ActorRefRoutee(r)
    }

    Router(RoundRobinRoutingLogic(), routees)
  }

  def receive = {
    case r: BrokerRequest =>
      router.route(r, sender())
    case Terminated(a) =>
      router = router.removeRoutee(a)
      val r = createBroker(a.path.name)
      router = router.addRoutee(r)
  }
}
