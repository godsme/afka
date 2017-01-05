package io.darwin.afka.services.domain

import java.net.InetSocketAddress

import akka.actor.FSM.Failure
import akka.actor.{ActorRef, FSM, Props, Terminated}
import io.darwin.afka.assignors.PartitionAssignor.MemberSubscription
import io.darwin.afka.assignors.RangeAssignor
import io.darwin.afka.decoder.decode
import io.darwin.afka.domain.FetchedMessages.{PartitionMessages, TopicMessages}
import io.darwin.afka.domain.GroupOffsets.GroupOffsets
import io.darwin.afka.domain.{FetchedMessages, GroupOffsets, KafkaCluster}
import io.darwin.afka.encoder.encode
import io.darwin.afka.packets.common.{ProtoMemberAssignment, ProtoPartitionAssignment, ProtoSubscription}
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses.{SyncGroupResponse, _}
import io.darwin.afka.services.common._
import io.darwin.afka.services.pool.PoolSinkChannel

import scala.collection.mutable.MutableList
import scala.concurrent.duration._

/**
  * Created by darwin on 26/12/2016.
  */

///////////////////////////////////////////////////////////////////////
object GroupCoordinator {

  def props( coordinator : Coordinator,
             clientId    : String = "",
             groupId     : String,
             cluster     : KafkaCluster,
             topics      : Array[String] ) = {
    Props(classOf[GroupCoordinator], coordinator, clientId, groupId, cluster, topics)
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
    this: Actor with KafkaServiceSinkChannel {
      val topics  : Array[String]
      val clientId: String
      val groupId : String
      val cluster : KafkaCluster
    } ⇒

    private var memberId   : Option[String] = None
    private var generation : Int            = 0

    def suicide(reason: String): State = {
      log.error(s"suicide: $reason")
      stop(Failure(reason))
    }

    startWith(CONNECTING, Dummy)


    when(DISCONNECTED, stateTimeout = 60 second) {
      case Event(StateTimeout, Dummy) ⇒ goto(CONNECTING)
    }

    when(CONNECTING, stateTimeout = 60 second) {
      case Event(ChannelConnected, Dummy) ⇒ joinGroup
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

    when(JOINED, stateTimeout = 5 second) {
      case Event(StateTimeout, Dummy)                  ⇒ heartBeat
      case Event(offsets: OffsetFetchResponse, Dummy)  ⇒ onOffsetFetched(offsets)
      case Event(commit: OffsetCommitResponse, Dummy)  ⇒ onCommitted(commit)
      case Event(r: HeartBeatResponse, Dummy) ⇒ {
        r.error match {
          case KafkaErrorCode.NO_ERROR                 ⇒ stay
          case KafkaErrorCode.UNKNOWN_MEMBER_ID        ⇒ {
            log.info(s"heart beat: code = ${r.error}")
            memberId = None
            joinGroup
          }
          case KafkaErrorCode.REBALANCE_IN_PROGRESS  |
               KafkaErrorCode.ILLEGAL_GENERATION       ⇒ {
            log.info(s"heart beat: code = ${r.error}")
            joinGroup
          }

          case KafkaErrorCode.GROUP_AUTHORIZATION_FAILED |
               KafkaErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE ⇒ {
            suicide(s"heart beat failed: ${r.error}")
          }
          case _ ⇒ {
            suicide(s"heart beat failed: ${r.error}")
          }
        }
      }
    }

    /////////////////////////////////////////////////////////////////
    val assigner = new RangeAssignor

    private def joinGroup: State = {
      stopFetchers

      sending(JoinGroupRequest(
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

      rsp.errorCode match {
        case KafkaErrorCode.NO_ERROR ⇒ {
          generation = rsp.generation
          memberId = Some(rsp.memberId)

          sending(sync(rsp))

          goto(PHASE2)
        }
        case KafkaErrorCode.NOT_COORDINATOR_FOR_GROUP ⇒ {
          log.error(s"@JoinGroupResponse: ${rsp.errorCode}, Kafka Server environment changed.")
          stop
        }
        case KafkaErrorCode.ILLEGAL_GENERATION |
             KafkaErrorCode.REBALANCE_IN_PROGRESS |
             KafkaErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE ⇒ {
          log.info(s"@JoinGroupResponse: ${rsp.errorCode}")
          joinGroup
        }
        case KafkaErrorCode.UNKNOWN_MEMBER_ID ⇒ {
          log.info(s"@JoinGroupResponse: ${rsp.errorCode}")
          memberId = None
          joinGroup
        }
        case _ ⇒ {
          suicide(s"@JoinGroupResponse: ${rsp.errorCode}")
          joinGroup
        }
      }
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
              assignment = encode(ProtoMemberAssignment(topics = assignments.toArray)))
        }

        SyncGroupRequest(groupId, generation, rsp.memberId, memberAssignment)
      }

      def syncAsFollower = {
        SyncGroupRequest(groupId, generation, rsp.memberId)
      }

      if(rsp.leaderId == rsp.memberId) syncAsLeader
      else syncAsFollower
    }

    var groups: Option[GroupOffsets] = None

    private def onSync(r: SyncGroupResponse) = {
      groups = r.assignment.map {
        case p ⇒ new GroupOffsets.GroupOffsets(cluster, p.topics)
      }

      def logMsg = {
        r.assignment.get.topics.foreach {
          case ProtoPartitionAssignment(topic, partitions) ⇒
            log.info(s"topic=${topic}, partitions={${partitions.mkString(", ")}}")
        }
      }

      def fetchOffset = {
        logMsg

        sending(OffsetFetchRequest(
          group  = groupId,
          topics = r.assignment.get.topics))

        goto(JOINED)
      }

      r.error match {
        case KafkaErrorCode.NO_ERROR ⇒ fetchOffset
        case e ⇒ {
          log.error(s"sync group failed ${e}")
          joinGroup
        }
      }
    }

    var fetchers: Array[ActorRef] = Array.empty

    private def stopFetchers = {
      fetchers.foreach(context.stop)
    }

    private def onOffsetFetched(offsets: OffsetFetchResponse) = {
      groups.foreach(_.update(offsets.topics))

      fetchers = groups.get.offsets.toArray.map {
        case (node, off) ⇒
          context.actorOf(FetchService.props(
            nodeId   = node,
            clientId = clientId,
            offsets  = off
        ), "fetcher-"+node)
      }

      stay
    }

    def autoCommit(msgs: MutableList[TopicMessages]) = {
      def sendCommit = {
        sending(OffsetCommitRequest(
          groupId    = groupId,
          generation = generation,
          consumerId = memberId.get,
          topics     = msgs.toArray.map {
            case TopicMessages(topic, msgs) ⇒
              TopicOffsetCommitRequest(topic, msgs.toArray.map {
                case PartitionMessages(partition, _, infos) ⇒
                  log.info(s"commit partition = ${partition}, offset = ${infos.get.last.offset}")
                  PartitionOffsetCommitRequest(partition, infos.get.last.offset)
              })
          }))

        stay
      }

      if(msgs.size > 0) sendCommit
    }

    def onCommitted(commit: OffsetCommitResponse) = {
      commit.topics.foreach {
        case OffsetCommitTopicResponse(topic, partitions) ⇒
          log.info(s"commit topic: ${topic}")
          partitions.foreach {
            case OffsetCommitPartitionResponse(partition, error) ⇒
              log.info(s"partition = ${partition}, error=${error}")
          }
      }

      stay
    }

    private def heartBeat = {
      sending(HeartBeatRequest(groupId, generation, memberId.get))
      stay
    }


    ////////////////////////////////////////////////////////////////////
    whenUnhandled {
      case Event(_: Terminated, Dummy) ⇒
        stopFetchers
        goto(DISCONNECTED)
      case Event(e: NotReady, _) ⇒
        stop
      case Event(e, _) ⇒
        log.info(s"${e}")
        stay
    }

    initialize()
  }
}

///////////////////////////////////////////////////////////////////////
class GroupCoordinator
  ( coordinator : Coordinator,
    val clientId    : String,
    val groupId     : String,
    val cluster     : KafkaCluster,
    val topics      : Array[String] )
  extends GroupCoordinator.Actor with PoolSinkChannel
{
  def path: String = "/user/push-service/cluster/broker-service/" + coordinator.nodeId
  //val remote = new InetSocketAddress(coordinator.host, coordinator.port)
}

