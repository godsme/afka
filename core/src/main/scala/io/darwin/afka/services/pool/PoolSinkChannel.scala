package io.darwin.afka.services.pool

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.contrib.pattern.ReceivePipeline
import akka.contrib.pattern.ReceivePipeline.Inner
import io.darwin.afka.packets.requests.KafkaRequest
import io.darwin.afka.services.common.{ChannelConnected, KafkaServiceSinkChannel}

/**
  * Created by darwin on 8/1/2017.
  */
trait PoolSinkChannel extends KafkaServiceSinkChannel with ReceivePipeline with ActorLogging {
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
    context watch ref
    Inner(ChannelConnected(sender()))
  }
}
