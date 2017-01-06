package io.darwin.afka.services.pool

import akka.actor.{Actor, ActorIdentity, ActorRef, ActorSelection, Identify}
import akka.contrib.pattern.ReceivePipeline
import akka.contrib.pattern.ReceivePipeline.{HandledCompletely, Inner}
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

  val identifyId = 1
  context.actorSelection(path) ! Identify(identifyId)

  override def sending[A <: KafkaRequest](req: A, from: ActorRef = self) = {
    target.fold { context.actorSelection(path) ! req } { to ⇒ to ! req }
  }

  pipelineOuter {
    case ActorIdentity(`identifyId`, c) ⇒
      c match {
        case Some(ref) ⇒
          target = c
          Inner(ChannelConnected(ref))
        case None ⇒
          context stop self
          HandledCompletely
      }
    case ResponsePacket(rsp, _) ⇒
      Inner(rsp)
  }
}
