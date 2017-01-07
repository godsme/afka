package io.darwin.afka.services.common

import akka.actor.Actor

/**
  * Created by darwin on 6/1/2017.
  */
trait AfkaActor {
  this: Actor ⇒

  def suicide(cause: String) = {

  }

}
