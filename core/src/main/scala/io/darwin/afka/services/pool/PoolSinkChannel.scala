package io.darwin.afka.services.pool

import akka.actor.{Actor, ActorRef, ActorSelection}
import io.darwin.afka.packets.requests.KafkaRequest
import io.darwin.afka.services.common.{ChannelConnected, KafkaActor, KafkaServiceSinkChannel, ResponsePacket}

/**
  * Created by darwin on 4/1/2017.
  */
trait PoolSinkChannel extends KafkaActor with KafkaServiceSinkChannel {
  this: {
    def path: String
  } ⇒

  private var target: Option[ActorRef] = None

  self ! ChannelConnected

  override def sending[A <: KafkaRequest](req: A, from: ActorRef = self) = {
    target.fold { context.actorSelection(path) ! req } { to ⇒ to ! req }
  }

  override def receive = {
    case ResponsePacket(rsp, _) ⇒ {
//      if(target.isEmpty) {
//        target = Some(sender())
//      }
      super.receive(rsp)
    }
    case e ⇒ super.receive(e)
  }

}
