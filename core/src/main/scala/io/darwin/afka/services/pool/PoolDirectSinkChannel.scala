package io.darwin.afka.services.pool

/**
  * Created by darwin on 4/1/2017.
  */
trait PoolDirectSinkChannel extends PoolSinkChannel {

  def path: String

  send(Echo)

  pipelineOuter {
    case Echo ⇒ onChannelReady(sender())
  }
}
