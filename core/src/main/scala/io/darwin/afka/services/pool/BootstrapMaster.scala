package io.darwin.afka.services.pool

import java.net.InetSocketAddress

import akka.actor.{ActorRef, Props}
import akka.routing.{RandomRoutingLogic, RoutingLogic}
import io.darwin.afka.services.common.{AfkaRouter, ReportOnFirstWorkerReady, RouterReadyReportStrategy}

/**
  * Created by darwin on 3/1/2017.
  */
object BootstrapMaster {
  def props( bootstraps : Array[InetSocketAddress], listener: ActorRef) = {
    Props(classOf[BootstrapMaster], bootstraps, listener)
  }
}

class BootstrapMaster(val bootstraps : Array[InetSocketAddress],
                      val listener   : ActorRef)
  extends AfkaRouter {

  def numOfWorkers: Int = bootstraps.length
  def routingLogic: RoutingLogic = RandomRoutingLogic()

  def createWorker(i: Int) = {
    (i, BrokerConnection.props(
        remote   = bootstraps(i),
        clientId = "bootstrap",
        listener = self))
  }

  def reportStrategy: RouterReadyReportStrategy = ReportOnFirstWorkerReady

  init
}