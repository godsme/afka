package io.darwin.afka.services

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.routing.{Router, RoutingLogic}

import scala.collection.mutable.Map

case object WorkerOnline
case class WorkerOffline(cause: String)

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
    def createWorker(i: Int): (Int, Props)
    def reportStrategy: RouterReadyReportStrategy
    val listener: ActorRef
  } ⇒

  private var workers: Map[Int, (ActorRef, Boolean)] = Map.empty
  private var router = Router(routingLogic, Vector.empty)

  def init = {
    for(i ← 0 until numOfWorkers) {
      addWorker(i)
    }
  }

  def addWorker(i: Int): Unit = {
    val (n, p) = createWorker(i)
    val r = context.actorOf(p, n.toString)
    workers += n → (r, false)
  }

  def restartWorker(n: Int, i: Int) = {
    workers.get(n) match {
      case None ⇒ addWorker(i)
      case Some(p) ⇒ {
        val (who, _) = p
        context.stop(who)
      }
    }
  }

  def removeWorker(n: Int) = {
    workers.get(n).fold(()) { case (worker, active) ⇒
      log.info(s"broker ${n} is removed")
      if(active) {
        router = router.removeRoutee(worker)
      }

      workers -= n
      context.stop(worker)
    }
  }

  override def receive: Receive = {
    case WorkerOnline               ⇒ onWorkerOnline
    case WorkerOffline(cause)       ⇒ onWorkerOffline(cause)
    case RoutingEvent(o: Any)       ⇒ onRoutingRequest(o)
    case Terminated(who: ActorRef)  ⇒ onWorkerDown(who)
  }

  private def onWorkerOnline = {
    def shouldReport: Boolean = {
      reportStrategy match {
        case ReportOnFirstWorkerReady ⇒ router.routees.size == 1
        case ReportOnAllWorkerReady   ⇒ router.routees.size == numOfWorkers
        case _                        ⇒ false
      }
    }

    router = router.addRoutee(sender())
    workers(sender().path.name.toInt) = (sender(), true)

    if(shouldReport) {
      listener ! WorkerOnline
    }
  }

  private def onWorkerOffline(cause: String) = {
    log.warning(s"worker ${sender} offline: ${cause}")
    router = router.removeRoutee(sender())
    val i = sender().path.name.toInt
    workers(sender().path.name.toInt) = (sender(), false)
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
    if(workers.get(i).isDefined) {
      workers -= i
      addWorker(i)
    }
  }

  def isWorkerReady(who: Int) = {
    workers(who)._2
  }
}
