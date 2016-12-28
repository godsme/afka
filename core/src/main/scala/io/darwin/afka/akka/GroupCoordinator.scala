package io.darwin.afka.akka

import java.net.InetSocketAddress

import akka.actor.{ActorRef, FSM, Props, Terminated}
import io.darwin.afka.packets.common.ConsumerGroupMeta
import io.darwin.afka.packets.requests.{GroupProtocol, HeartBeatRequest, JoinGroupRequest}
import io.darwin.afka.packets.responses.{HeartBeatResponse, JoinGroupResponse}

import scala.concurrent.duration._

/**
  * Created by darwin on 26/12/2016.
  */

///////////////////////////////////////////////////////////////////////
object GroupCoordinator {

  def props( remote: InetSocketAddress,
             clientId: String = "coord",
             topics: Array[String] ) = {
    Props(classOf[GroupCoordinator], remote, clientId, topics)
  }

  sealed trait State
  case object DISCONNECTED extends State
  case object CONNECTING   extends State
  case object JOINING      extends State
  case object JOINED       extends State

  sealed trait Data
  case object Dummy extends Data

  trait Actor extends FSM[State, Data] {
    this: Actor with KafkaService {
      val topics: Array[String]
    } ⇒

    private var memberId: Option[String] = None
    private var generation: Int = 0

    startWith(CONNECTING, Dummy)

    when(DISCONNECTED, stateTimeout = 60 second) {
      case Event(StateTimeout, Dummy) ⇒ {
        goto(CONNECTING)
      }
    }

    when(CONNECTING, stateTimeout = 60 second) {
      case Event(KafkaClientConnected(_), Dummy) ⇒
        joinGroup
        goto(JOINING)
    }

    when(JOINING) {
      case Event(r: JoinGroupResponse, Dummy) ⇒ {
        joined(r)
        goto(JOINED)
      }
    }

    when(JOINED, stateTimeout = 10 second) {
      case Event(StateTimeout, Dummy) ⇒ {
        heartBeat
        stay
      }
      case Event(rsp:HeartBeatResponse, Dummy) ⇒ {
        log.info(s"heart beat error=${rsp.error}")
        stay
      }
    }

    private def joinGroup = {
      val groupMeta = ByteStringSinkChannel().encodeWithoutSize(ConsumerGroupMeta(subscription = topics))
      send(JoinGroupRequest(groupId="my-group", protocols=Array(GroupProtocol(meta=groupMeta))))
    }

    private def joined(rsp: JoinGroupResponse) = {
      log.info(s"error=${rsp.errorCode}, " +
        s"generation=${rsp.generation}, " +
        s"proto=${rsp.groupProtocol}, " +
        s"leader=${rsp.leaderId}, " +
        s"member=${rsp.memberId}, " +
        s"members=${rsp.members.mkString(",")}")

      generation = rsp.generation
      memberId = Some(rsp.memberId)
    }

    private def heartBeat = {
      val beat = ByteStringSinkChannel().encodeWithoutSize(ConsumerGroupMeta(subscription = topics))
      val packet = HeartBeatRequest(groupId="my-group", generation=generation,memberId=memberId.get)
      send(packet)
    }

    whenUnhandled {
      case Event(_: Terminated, Dummy) ⇒
        goto(DISCONNECTED)

      case Event(_, _) ⇒
        stay
    }

    initialize()
  }
}


///////////////////////////////////////////////////////////////////////
class GroupCoordinator
  ( val remote: InetSocketAddress,
    val clientId: String,
    val topics: Array[String])
  extends KafkaActor with GroupCoordinator.Actor with KafkaService

