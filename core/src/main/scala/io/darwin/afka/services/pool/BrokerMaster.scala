package io.darwin.afka.services.pool

import java.net.InetSocketAddress

import akka.actor.{ActorRef, Props}
import akka.routing._
import io.darwin.afka.services.common.{AfkaRouter, ReportOnFirstWorkerReady, RouterReadyReportStrategy}

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
}

class BrokerMaster( val remote   : InetSocketAddress,
                    val clientId : String,
                    val node     : Int,
                    val listener : ActorRef)
  extends AfkaRouter {

  def numOfWorkers: Int = 2

  def routingLogic: RoutingLogic = RoundRobinRoutingLogic()

  def createWorker(i: Int) = (i, BrokerConnection.props(remote, clientId, self))

  def reportStrategy: RouterReadyReportStrategy = ReportOnFirstWorkerReady

  init
}
