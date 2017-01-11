package io.darwin.afka.services.pool

import akka.actor.Actor
import akka.contrib.pattern.ReceivePipeline.Inner

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
  }
}
