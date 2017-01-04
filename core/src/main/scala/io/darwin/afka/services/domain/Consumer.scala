package io.darwin.afka.services.domain

import akka.actor.{FSM, Props}
import io.darwin.afka.TopicId
import io.darwin.afka.domain.KafkaCluster
import io.darwin.afka.packets.requests.MetaDataRequest
import io.darwin.afka.packets.responses.MetaDataResponse
import io.darwin.afka.services.domain.MetaDataService.SessionMetaData

import scala.concurrent.duration._

/**
  * Created by darwin on 2/1/2017.
  */
object Consumer {
  def props( group  : String,
             topics : Array[TopicId]) =  Props(classOf[Consumer], group, topics)

  sealed trait State1
  case object DISCONNECT   extends State1
  case object CONNECTING   extends State1

  sealed trait Data
  case object Dummy extends Data
}

import Consumer._

class Consumer(val group: String, val topics: Array[TopicId])
  extends FSM[State1, Data] {

  startWith(DISCONNECT, Dummy)

  context.actorOf(MetaDataService.props("", group, topics, self), "meta-data-fetcher")

  when(DISCONNECT) {
    case Event(StateTimeout, _) ⇒
      //context.actorSelection("/user/push-service/cluster") ! MetaDataRequest(Some(topics))
      stay
    case Event(SessionMetaData(meta, coordinator), _) ⇒
      context watch context.actorOf(GroupCoordinator.props(
                    coordinator   = coordinator,
                    clientId = "",
                    groupId  = group,
                    cluster  = KafkaCluster(meta),
                    topics   = topics), "coordinator")
      goto(CONNECTING)
  }

  when(CONNECTING) {
    case e ⇒
      log.info(s"${e}")
      stay
  }

  whenUnhandled {
    case Event(Unreachable(to, msg), _) ⇒ {
      log.info(s"${to} unreachable, ${msg} is not delivered")
      stay
    }
    case e ⇒
      log.info(s"${e}")
      stay()
  }

  initialize()
}
