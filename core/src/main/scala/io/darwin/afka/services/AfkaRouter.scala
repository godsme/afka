package io.darwin.afka.services

import akka.actor.{Actor, ActorLogging, ActorRef, Terminated}
import akka.routing.{Router, RoutingLogic}

import scala.collection.mutable.Map

case class WorkerReady(who: ActorRef)
case class NotReady(any: Any)
case class RoutingEvent(event: Any)

sealed trait RouterReadyReportStrategy
case object ReportNothing            extends RouterReadyReportStrategy
case object ReportOnFirstWorkerReady extends RouterReadyReportStrategy
case object ReportOnAllWorkerReady   extends RouterReadyReportStrategy

/**
  * Created by darwin on 2/1/2017.
  */
trait AfkaRouter extends Actor with ActorLogging {
  this: {
    def numOfWorkers: Int
    def routingLogic: RoutingLogic
    def createWorker(i: Int): ActorRef
    def reportStrategy: RouterReadyReportStrategy
    val listener: ActorRef
  } ⇒

  private var workers: Map[Int, (ActorRef, Boolean)] = Map.empty
  private var router = Router(routingLogic, Vector.empty)

  for(i ← 0 until numOfWorkers) {
    val r = createWorker(i)
    workers += i → (r, false)
  }

  override def receive: Receive = {
    case WorkerReady(who: ActorRef) ⇒ onWorkerReady(who)
    case RoutingEvent(o: Any)       ⇒ onRoutingRequest(o)
    case Terminated(who: ActorRef)  ⇒ onWorkerDown(who)
  }

  private def onWorkerReady(who: ActorRef) = {
    def shouldReport: Boolean = {
      reportStrategy match {
        case ReportOnFirstWorkerReady ⇒ router.routees.size == 1
        case ReportOnAllWorkerReady   ⇒ router.routees.size == numOfWorkers
        case _                        ⇒ false
      }
    }

    router = router.addRoutee(who)
    workers(who.path.name.toInt) = (who, true)

    if(shouldReport) {
      listener ! WorkerReady(self)
    }
  }

  private def onRoutingRequest(o: Any) = {
    if(!send(o)) {
      sender() ! NotReady(o)
    }
  }

  def send(o: Any): Boolean = {
    if(router.routees.size == 0) return false
    router.route(RoutingEvent(o), sender())
    true
  }

  private def onWorkerDown(who: ActorRef) = {
    router = router.removeRoutee(who)
    val i = who.path.name.toInt
    workers -= i
    val r = createWorker(i)
    workers += i -> (r, false)
  }

  def isWorkerReady(who: Int) = {
    workers(who)._2
  }
}
