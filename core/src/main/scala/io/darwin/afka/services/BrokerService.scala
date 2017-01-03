package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{ActorRef, Props}
import akka.routing._
import io.darwin.afka.domain.{Broker, Brokers}

/**
  * Created by darwin on 2/1/2017.
  */
object BrokerService {
  def props( brokers    : Brokers,
             clientId   : String,
             listener   : ActorRef ) = {
    Props(classOf[BrokerService], brokers, clientId, listener)
  }
}

class BrokerService( brokers_ : Brokers,
                     val clientId : String,
                     val listener : ActorRef)
  extends AfkaRouter {

  private var brokers = brokers_

  def numOfWorkers: Int = brokers.length
  def routingLogic: RoutingLogic = RandomRoutingLogic()

  def createWorker(i: Int) = {
    val Broker(node, host, port, _) = brokers(i)
    (node, BrokerMaster.props(
             remote   = new InetSocketAddress(host, port),
             clientId = "push-service",
             node     = node,
             listener = self))
  }

  def reportStrategy: RouterReadyReportStrategy = ReportOnAllWorkerReady

  override def receive: Receive = {
    case e: Brokers ⇒ {
      val old = brokers
      brokers = e

      for(i ← 0 until old.length) {
        if(brokers.get(old(i).nodeId).isEmpty) {
          removeWorker(old(i).nodeId)
        }
      }

      for(i ← 0 until brokers.length) {
        val broker = brokers.brokers(i)
        if(old.get(broker.nodeId).fold(true)(b ⇒ b != broker))
          restartWorker(broker.nodeId, i)
      }
    }
    case e ⇒ super.receive(e)
  }

  init
}
