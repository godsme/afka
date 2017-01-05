package io.darwin.afka.services.pool

import akka.actor.{Actor, ActorRef, ActorSelection}
import akka.contrib.pattern.ReceivePipeline
import akka.contrib.pattern.ReceivePipeline.Inner
import io.darwin.afka.packets.requests.KafkaRequest
import io.darwin.afka.services.common.{ChannelConnected, KafkaServiceSinkChannel, ResponsePacket}

/**
  * Created by darwin on 4/1/2017.
  */
trait PoolSinkChannel extends KafkaServiceSinkChannel with ReceivePipeline {
  this: {
    def path: String
  } ⇒

  private var target: Option[ActorRef] = None

  self ! ChannelConnected

  override def sending[A <: KafkaRequest](req: A, from: ActorRef = self) = {
    target.fold { context.actorSelection(path) ! req } { to ⇒ to ! req }
  }

  pipelineOuter {
    case ResponsePacket(rsp, _) ⇒ {
      Inner(rsp)
    }
  }
}
