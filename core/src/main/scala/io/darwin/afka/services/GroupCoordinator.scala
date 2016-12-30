package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{ActorRef, FSM, Props, Terminated}
import io.darwin.afka.{NodeId, PartitionId, TopicId}
import io.darwin.afka.assignors.PartitionAssignor.MemberSubscription
import io.darwin.afka.assignors.RangeAssignor
import io.darwin.afka.domain.KafkaCluster
import io.darwin.afka.packets.common.{ProtoMemberAssignment, ProtoPartitionAssignment, ProtoSubscription}
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses.{SyncGroupResponse, _}
import io.darwin.afka.decoder.decode
import io.darwin.afka.encoder.encode
import io.darwin.afka.services.FetchService.TopicAssignment

import scala.collection.mutable
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
      val topics  : Array[String]
      val clientId: String
      val groupId : String
      val cluster : KafkaCluster
    } ⇒

    private var memberId   : Option[String] = None
    private var generation : Int            = 0

    def suicide(reason: String): State = {
      log.error(s"suicide: $reason")
      context stop self
      stay
    }

    startWith(CONNECTING, Dummy)


    when(DISCONNECTED, stateTimeout = 60 second) {
      case Event(StateTimeout, Dummy) ⇒ goto(CONNECTING)
    }


    when(CONNECTING, stateTimeout = 60 second) {
      case Event(KafkaClientConnected(_), Dummy) ⇒ joinGroup
    }

    when(PHASE1) {
      case Event(r: JoinGroupResponse, Dummy)    ⇒ onJoined(r)
    }

    when(PHASE2) {
      case Event(r: SyncGroupResponse, Dummy) ⇒ {
        r.error match {
          case KafkaErrorCode.NO_ERROR               ⇒ onSync(r)
          case KafkaErrorCode.UNKNOWN_MEMBER_ID  |
               KafkaErrorCode.ILLEGAL_GENERATION |
               KafkaErrorCode.REBALANCE_IN_PROGRESS  ⇒ joinGroup

          // case KafkaErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE ⇒
          // case KafkaErrorCode.NOT_COORDINATOR_FOR_GROUP ⇒
          // case KafkaErrorCode.GROUP_AUTHORIZATION_FAILED ⇒
          case e ⇒ suicide(s"SyncGroup failed: ${e}")
        }
      }
    }


    when(JOINED, stateTimeout = 8 second) {
      case Event(StateTimeout, Dummy)                 ⇒ heartBeat
      case Event(r: HeartBeatResponse, Dummy) ⇒ {
        log.info(s"heart beat: code = ${r.error}")
        r.error match {
          case KafkaErrorCode.NO_ERROR                ⇒ stay
          case KafkaErrorCode.ILLEGAL_GENERATION |
               KafkaErrorCode.UNKNOWN_MEMBER_ID  |
               KafkaErrorCode.REBALANCE_IN_PROGRESS   ⇒ joinGroup

          case KafkaErrorCode.GROUP_AUTHORIZATION_FAILED |
               KafkaErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE ⇒ {
            suicide(s"HearBeat failed: ${r.error}")
          }
        }
      }
    }

    /////////////////////////////////////////////////////////////////
    val assigner = new RangeAssignor


    private def joinGroup: State = {
      send(JoinGroupRequest(
        groupId   = groupId,
        memberId  = memberId.getOrElse(""),
        protocols = Array(GroupProtocol(
          name = assigner.name,
          meta = encode(assigner.subscribe(topics))))))

      goto(PHASE1)
    }

    private def onJoined(rsp: JoinGroupResponse) = {
      log.info(
        s"error = ${rsp.errorCode}, " +
          s"generation = ${rsp.generation}, " +
          s"proto = ${rsp.groupProtocol}, " +
          s"leader = ${rsp.leaderId}, " +
          s"member = ${rsp.memberId}, " +
          s"members = ${rsp.members.getOrElse(Array.empty).mkString(",")}")

      generation = rsp.generation
      memberId = Some(rsp.memberId)

      send(sync(rsp))

      goto(PHASE2)
    }

    private def sync(rsp: JoinGroupResponse) = {
      def syncAsLeader = {
        val subscription = rsp.members.get.map {
          case GroupMember(id, meta) ⇒
            MemberSubscription(id, decode[ProtoSubscription](meta))
        }

        val memberAssignment = assigner.assign(cluster, subscription).toArray.map {
          case (member, assignments) ⇒
            GroupAssignment(
              member     = member,
              assignment = encode(ProtoMemberAssignment(assignment = assignments.toArray)))
        }

        SyncGroupRequest(groupId, generation, rsp.memberId, memberAssignment)
      }

      def syncAsFollower = {
        SyncGroupRequest(groupId, generation, rsp.memberId)
      }

      if(rsp.leaderId == rsp.memberId) syncAsLeader
      else syncAsFollower
    }

    private var fetchers: Array[ActorRef] = Array.empty

    private def onSync(r: SyncGroupResponse) = {
      val m = decode[ProtoMemberAssignment](r.assignment)

      type TopicMap = mutable.Map[TopicId, mutable.MutableList[PartitionId]]

      val routes: mutable.Map[NodeId, TopicMap] = mutable.Map.empty

      m.assignment.foreach {
        case ProtoPartitionAssignment(topic, partitions) ⇒
          log.info(s"${topic}: ${partitions.mkString(",")}")
          val partitionMap = cluster.getPartitionMapByTopic(topic).getOrElse(Map.empty)

          partitions.foreach { p ⇒
            routes.getOrElseUpdate(partitionMap(p).leader, mutable.Map.empty)
              .getOrElseUpdate(topic, mutable.MutableList.empty)
              .+=(p)
          }
      }

      fetchers = routes.toArray.flatMap {
        case (node, topics) ⇒
          cluster.getBroker(node).map { remote ⇒
            context.actorOf(FetchService.props(
              remote   = remote,
              clientId = clientId,
              groupId  = groupId,
              topics   = topics.toArray.map {
                case (topic, partitions) ⇒ TopicAssignment(topic, partitions.toArray)
              }))
          }
      }

      goto(JOINED)
    }

    private def heartBeat = {
      send(HeartBeatRequest(groupId, generation, memberId.get))
      stay
    }

    ////////////////////////////////////////////////////////////////////
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

