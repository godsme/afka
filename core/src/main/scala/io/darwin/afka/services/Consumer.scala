package io.darwin.afka.services

import akka.actor.{FSM, Props, Terminated}
import io.darwin.afka.TopicId
import io.darwin.afka.services.ClusterService.CreateConsumer

import scala.concurrent.duration._
/**
  * Created by darwin on 2/1/2017.
  */
object Consumer {
  def props( group  : String,
             topics : Array[TopicId]) = {
    Props(classOf[Consumer], group, topics)
  }

  sealed trait State1
  case object DISCONNECT   extends State1
  case object CONNECTING   extends State1

  sealed trait Data
  case object Dummy extends Data
}

import Consumer._

class Consumer(val group: String, val topics: Array[TopicId])
  extends FSM[State1, Data] {


  setTimer("trigger", StateTimeout, 2 second, false)

  log.info("Consumer ....")
  startWith(DISCONNECT, Dummy, Some(2 second))

  when(DISCONNECT, stateTimeout = 2 second) {
    case Event(StateTimeout, _) ⇒
      log.info("send create consumer")
      context.actorSelection("/user/push-service/cluster") ! CreateConsumer(group, topics)
      goto(CONNECTING)
    case e ⇒
      log.info(s"${e}")
      stay
  }

  when(CONNECTING) {
    case e ⇒
      log.info(s"${e}")
      stay
  }

  whenUnhandled {
    case e ⇒
      log.info(s"${e}")
      stay()
  }

}
