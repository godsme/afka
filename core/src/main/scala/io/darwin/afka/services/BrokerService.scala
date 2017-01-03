package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{ActorRef, Props}
import akka.routing._
import io.darwin.afka.packets.responses.BrokerResponse

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
  extends AfkaRouter {

  def numOfWorkers: Int = brokers.length
  def routingLogic: RoutingLogic = RandomRoutingLogic()

  def createWorker(i: Int): ActorRef = {
    val BrokerResponse(node, host, port, _) = brokers(i)
    context.actorOf(BrokerMaster.props(
        remote   = new InetSocketAddress(host, port),
        clientId = "push-service",
        node     = node,
        listener = self),
      node.toString)
  }

  def reportStrategy: RouterReadyReportStrategy = ReportOnAllWorkerReady
}
