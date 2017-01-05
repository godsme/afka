package io.darwin.afka.services.domain

import akka.actor.{ActorRef, FSM, Props, Terminated}
import io.darwin.afka.TopicId
import io.darwin.afka.domain.KafkaCluster
import io.darwin.afka.services.domain.MetaDataService.SessionMetaData

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

import io.darwin.afka.services.domain.Consumer._

class Consumer(val group: String, val topics: Array[TopicId])
  extends FSM[State1, Data] {

  startWith(DISCONNECT, Dummy)

  var metaFetcher: Option[ActorRef] = None
  var coordinator: Option[ActorRef] = None

  startMetaFetcher

  def startMetaFetcher = {
    metaFetcher = Some(context.actorOf(MetaDataService.props("", group, topics, self), "meta-data-fetcher"))
    context watch metaFetcher.get
  }

  when(DISCONNECT) {
    case Event(StateTimeout, _) ⇒
      //context.actorSelection("/user/push-service/cluster") ! MetaDataRequest(Some(topics))
      stay
    case Event(SessionMetaData(meta, c), _) ⇒
      coordinator =
        Some(context.actorOf(GroupCoordinator.props(
                    coordinator   = c,
                    clientId = "",
                    groupId  = group,
                    cluster  = KafkaCluster(meta),
                    topics   = topics), "coordinator"))
      context watch coordinator.get
      context unwatch metaFetcher.get
      goto(CONNECTING)
    case Event(_:Terminated, _) ⇒ {
      stop
    }
  }

  when(CONNECTING) {
    case Event(_:Terminated, _) ⇒ {
      stop
    }
    case e ⇒
      log.info(s"${e}")
      stay
  }

  whenUnhandled {
    case Event(Unreachable(to, msg), _) ⇒ {
      log.info(s"${to} unreachable, ${msg} is not delivered")
      stop
    }
    case e ⇒
      log.info(s"${e}")
      stay()
  }

  initialize()
}
