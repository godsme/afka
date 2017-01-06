package io.darwin.afka.services.domain

import akka.actor.{Actor, ActorLogging, ActorRef, DeadLetter}


case class Unreachable(where: ActorRef, msg: Any)
/**
  * Created by darwin on 3/1/2017.
  */
class DeadLetterListener extends Actor with ActorLogging {

  override def preStart {
    context.system.eventStream.subscribe(self, classOf[DeadLetter])
  }

  override def postStop {
    context.system.eventStream.unsubscribe(self)
  }

  def receive = {
    case DeadLetter(msg, from, to) =>
      if(from != self && !from.path.toString.startsWith("akka://default/user/push-service/cluster"))
        from ! Unreachable(to, msg)

  }
}
