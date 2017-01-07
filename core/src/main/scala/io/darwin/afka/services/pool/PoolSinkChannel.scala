package io.darwin.afka.services.pool

import akka.actor.{Actor, ActorRef}
import akka.contrib.pattern.ReceivePipeline
import akka.contrib.pattern.ReceivePipeline.Inner
import io.darwin.afka.packets.requests.KafkaRequest
import io.darwin.afka.services.common.{ChannelConnected, KafkaServiceSinkChannel, ResponsePacket}

/**
  * Created by darwin on 8/1/2017.
  */
trait PoolSinkChannel extends KafkaServiceSinkChannel with ReceivePipeline {
  this: Actor {
    def path: String
  } ⇒

  private var target: Option[ActorRef] = None

  protected def send(o: Any) = {
    target.fold {
      context.actorSelection(path) ! o
    } { to ⇒ to ! o }
  }

  override def sending[A <: KafkaRequest](req: A, from: ActorRef = self) = {
    send(req)
  }

  protected def onChannelReady(ref: ActorRef) = {
    target = Some(ref)
    Inner(ChannelConnected(sender()))
  }
}
