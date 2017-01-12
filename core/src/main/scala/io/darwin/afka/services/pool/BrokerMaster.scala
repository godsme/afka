package io.darwin.afka.services.pool

import java.net.InetSocketAddress

import akka.actor.ActorRef
import akka.routing._
import io.darwin.afka.macros.ActorObject
import io.darwin.afka.services.common.{AfkaRouter, ReportOnFirstWorkerReady, RouterReadyReportStrategy}

/**
  * Created by darwin on 2/1/2017.
  */
@ActorObject
class BrokerMaster( val remote   : InetSocketAddress,
                    val clientId : String,
                    val node     : Int,
                    val listener : ActorRef)
  extends AfkaRouter {

  def numOfWorkers: Int = 3
  def routingLogic: RoutingLogic = RoundRobinRoutingLogic()
  def createWorker(i: Int) = (i, BrokerConnection.props(remote, clientId, self))
  def reportStrategy: RouterReadyReportStrategy = ReportOnFirstWorkerReady

  init
}
