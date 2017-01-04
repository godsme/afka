package io.darwin.afka.services.common

import akka.actor.{Actor, ActorRef}
import io.darwin.afka.packets.requests.KafkaRequest

case object ChannelConnected
/**
  * Created by darwin on 4/1/2017.
  */
trait KafkaServiceSinkChannel {
  this: Actor ⇒

  def sending[A <: KafkaRequest](req: A, from: ActorRef = self)
}
