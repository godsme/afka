package io.darwin.afka.akka

import akka.actor.{Actor, ActorLogging, PoisonPill}

/**
  * Created by darwin on 26/12/2016.
  */
class BootStrap extends Actor with ActorLogging {

  override def receive: Receive = {
    case m => log.info(s"recv: ${m.toString}")
    //context.stop(self)
    self ! PoisonPill
  }
}
