package io.darwin.afka.services.domain

import akka.actor.{Actor, ActorRef, DeadLetter}


case class Unreachable(where: ActorRef, msg: Any)
/**
  * Created by darwin on 3/1/2017.
  */
class DeadLetterListener extends Actor {
  override def preStart {
    context.system.eventStream.subscribe(self, classOf[DeadLetter])
  }

  override def postStop {
    context.system.eventStream.unsubscribe(self)
  }

  def receive = {
    case m@DeadLetter(msg, from, to) =>
      if(from != self)
        from ! Unreachable(to, msg)
  }
}
