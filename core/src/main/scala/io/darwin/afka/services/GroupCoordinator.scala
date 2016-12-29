package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{FSM, Props, Terminated}
import io.darwin.afka.assignors.PartitionAssignor.{MemberAssignment, MemberSubscription}
import io.darwin.afka.assignors.RangeAssignor
import io.darwin.afka.domain.KafkaCluster
import io.darwin.afka.packets.common.{ProtoMemberAssignment, ProtoPartitionAssignment, ProtoSubscription}
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses._
import io.darwin.afka.decoder.decode
import io.darwin.afka.encoder.encode

import scala.concurrent.duration._

/**
  * Created by darwin on 26/12/2016.
  */

///////////////////////////////////////////////////////////////////////
object GroupCoordinator {

  def props( remote   : InetSocketAddress,
             clientId : String,
             groupId  : String,
             cluster  : KafkaCluster,
             topics   : Array[String] ) = {
    Props(classOf[GroupCoordinator], remote, clientId, groupId, cluster, topics)
  }

  sealed trait State
  case object DISCONNECTED extends State
  case object CONNECTING   extends State
  case object PHASE1       extends State
  case object PHASE2       extends State
  case object JOINED       extends State

  sealed trait Data
  case object Dummy extends Data

  trait Actor extends FSM[State, Data] {
    this: Actor with KafkaService {
      //val remote  : InetSocketAddress
      val topics  : Array[String]
      val groupId : String
      val cluster : KafkaCluster
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
        goto(PHASE1)
    }

    when(PHASE1) {
      case Event(r: JoinGroupResponse, Dummy) ⇒ {
        joined(r)
        goto(PHASE2)
      }
    }

    when(PHASE2) {
      case Event(r: SyncGroupResponse, Dummy) ⇒ {
        log.info(s"SyncGroupResponse = ${r.error}")
        val m = decode[ProtoMemberAssignment](r.assignment)
        m.assignment.foreach {
          case ProtoPartitionAssignment(topic, partitions, _) ⇒
            log.info(s"${topic}: ${partitions.mkString(",")}")
        }

        goto(JOINED)
      }
    }

    when(JOINED, stateTimeout = 8 second) {
      case Event(StateTimeout, Dummy) ⇒ {
        heartBeat
        stay
      }
      case Event(r:HeartBeatResponse, Dummy) ⇒ {
        log.info(s"heart beat error=${r.error}")
        r.error match {
          case KafkaErrorCode.NO_ERROR ⇒ {
            stay
          }
          case KafkaErrorCode.ILLEGAL_GENERATION |
               KafkaErrorCode.UNKNOWN_MEMBER_ID |
               KafkaErrorCode.REBALANCE_IN_PROGRESS ⇒ {
            joinGroup
            goto(PHASE1)
          }
          case KafkaErrorCode.GROUP_AUTHORIZATION_FAILED |
               KafkaErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE ⇒ {
            context stop self
            stay
          }
        }
      }
    }

    val assigner = new RangeAssignor

    private def joinGroup = {
      val req = JoinGroupRequest(
        groupId,
        memberId = memberId.getOrElse(""),
        protocols = Array(GroupProtocol(
          name = assigner.name,
          meta = encode(assigner.subscribe(topics)))))

      send(req)
    }

    private def joined(rsp: JoinGroupResponse) = {
      log.info(
        s"error = ${rsp.errorCode}, " +
        s"generation = ${rsp.generation}, " +
        s"proto = ${rsp.groupProtocol}, " +
        s"leader = ${rsp.leaderId}, " +
        s"member = ${rsp.memberId}, " +
        s"members = ${rsp.members.getOrElse(Array.empty).mkString(",")}")

      generation = rsp.generation
      memberId = Some(rsp.memberId)

      if(rsp.leaderId == rsp.memberId) {
        val subscription = rsp.members.get.map {
          case GroupMember(id, meta) ⇒ MemberSubscription(id, decode[ProtoSubscription](meta))
        }

        val memberAssignment =
          assigner.assign(cluster, subscription).toArray.map {
            case (member, assignments) ⇒
              val assign = encode(ProtoMemberAssignment(assignment = assignments.toArray))
              GroupAssignment(member, assign)
          }

        val sync = SyncGroupRequest(groupId, generation, rsp.memberId, memberAssignment)
        send(sync)
      }
      else{
        val sync = SyncGroupRequest(groupId, generation, rsp.memberId)
        send(sync)
      }
    }

    private def heartBeat = {
      val beat   = ByteStringSinkChannel().encodeWithoutSize(ProtoSubscription(topics = topics))
      val packet = HeartBeatRequest(groupId, generation, memberId.get)
      log.info(s"heat beat: $groupId $generation ${memberId.get}")
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
  ( val remote   : InetSocketAddress,
    val clientId : String,
    val groupId  : String,
    val cluster  : KafkaCluster,
    val topics   : Array[String] )
  extends KafkaActor with GroupCoordinator.Actor with KafkaService

