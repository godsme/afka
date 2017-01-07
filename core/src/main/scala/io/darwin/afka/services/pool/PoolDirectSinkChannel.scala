package io.darwin.afka.services.pool

import akka.actor.Actor
import akka.contrib.pattern.ReceivePipeline.Inner
import io.darwin.afka.services.common.ResponsePacket

/**
  * Created by darwin on 4/1/2017.
  */
trait PoolDirectSinkChannel extends PoolSinkChannel {
  this: Actor {
    def path: String
  } ⇒

  send(Echo)

  pipelineOuter {
    case Echo ⇒ {
      onChannelReady(sender())
    }
    case ResponsePacket(rsp, _) ⇒
      Inner(rsp)
  }
}
