package io.darwin.afka.services.pool

import akka.actor.{ActorIdentity, Identify}
import akka.contrib.pattern.ReceivePipeline.HandledCompletely

/**
  * Created by darwin on 4/1/2017.
  */
trait PoolDynamicSinkChannel extends PoolSinkChannel {

  def path: String

  val identifyId = 1
  send(Identify(identifyId))

  pipelineOuter {
    case ActorIdentity(`identifyId`, c) ⇒
      c match {
        case Some(ref) ⇒
          onChannelReady(ref)
        case None ⇒
          context stop self
          HandledCompletely
      }
  }

}
