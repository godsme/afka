package io.darwin.afka.services

import akka.actor.Actor

/**
  * Created by darwin on 3/1/2017.
  */
trait KafkaActor extends Actor {
  override def receive: Receive = {
    case _ ⇒ throw new Exception("an actor should implemented receive")
  }
}

