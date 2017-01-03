package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.FSM.Failure
import akka.actor.{ActorRef, FSM, Props, Terminated}
import akka.util.ByteString
import io.darwin.afka.assignors.PartitionAssignor.MemberSubscription
import io.darwin.afka.assignors.RangeAssignor
import io.darwin.afka.domain.{FetchedMessages, GroupOffsets, KafkaCluster}
import io.darwin.afka.packets.common.{ProtoMemberAssignment, ProtoMessageInfo, ProtoPartitionAssignment, ProtoSubscription}
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses.{SyncGroupResponse, _}
import io.darwin.afka.decoder.{decode, decoding}
import io.darwin.afka.domain.FetchedMessages.{PartitionMessages, TopicMessages}
import io.darwin.afka.domain.GroupOffsets.{GroupOffsets, PartitionOffsetInfo}
import io.darwin.afka.encoder.encode

import scala.collection.mutable.MutableList
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
      stop(Failure(reason))
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
      case Event(StateTimeout, Dummy)                  ⇒ heartBeat
      case Event(offsets: OffsetFetchResponse, Dummy)  ⇒ onOffsetFetched(offsets)
      case Event(msg: FetchResponse, Dummy)            ⇒ onMessageFetched(msg)
      case Event(commit: OffsetCommitResponse, Dummy)  ⇒ onCommitted(commit)
      case Event(r: HeartBeatResponse, Dummy) ⇒ {
        r.error match {
          case KafkaErrorCode.NO_ERROR                 ⇒ stay
          case KafkaErrorCode.ILLEGAL_GENERATION |
               KafkaErrorCode.UNKNOWN_MEMBER_ID  |
               KafkaErrorCode.REBALANCE_IN_PROGRESS    ⇒ {
            log.info(s"heart beat: code = ${r.error}")
            joinGroup
          }

          case KafkaErrorCode.GROUP_AUTHORIZATION_FAILED |
               KafkaErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE ⇒ {
            suicide(s"heart beat failed: ${r.error}")
          }
        }
      }
    }

    /////////////////////////////////////////////////////////////////
    val assigner = new RangeAssignor


    private def joinGroup: State = {
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

      generation = rsp.generation
      memberId = Some(rsp.memberId)

      sending(sync(rsp))

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
            log.info(s"topic=${topic}, paritions={${partitions.mkString(", ")}}")
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

    private def fetchMessages(offsets: OffsetFetchResponse) = {

      def logFailedInfos = {
        offsets.topics.foreach {
          case OffsetFetchTopicResponse(topic, partitions) ⇒
            partitions.filter(_.error != KafkaErrorCode.NO_ERROR).foreach {
              case OffsetFetchPartitionResponse(part, offset, _, error) ⇒
                log.info(s"fetch offset error = ${error} on ${topic}: ${part} : ${offset}")
            }
        }
      }

      def fetchingTopics: Array[FetchTopicRequest] = {
        offsets.topics.map {
          case OffsetFetchTopicResponse(topic, partitions) ⇒
            FetchTopicRequest(
              topic      = topic,
              partitions = partitions.filter(p ⇒ p.error == KafkaErrorCode.NO_ERROR).map {
                case OffsetFetchPartitionResponse(partition, offset, _, _) ⇒
                  FetchPartitionRequest(partition, offset+1)
              })
        }
      }

      def fetchTopics(topics: Array[FetchTopicRequest]) = {
        def logging = {
          val s: Array[String] = topics.map { s ⇒
            s.topic + " : " + s.partitions.map { p ⇒
              p.partition + ":" + p.offset
            }.mkString(",")
          }

          log.info(s"send fetch request: ${s.mkString(",")}")
        }

        if(topics.size > 0) {
          logging
          sending(FetchRequest(topics = topics))
        }
      }

      logFailedInfos
      fetchTopics(fetchingTopics.filter(_.partitions.size > 0))
    }

    private def onOffsetFetched(offsets: OffsetFetchResponse) = {
      groups.foreach(_.update(offsets.topics))

      //fetchMessages(offsets)
      groups.foreach {
        case g ⇒ g.toRequests.foreach {
          case (node, req) ⇒
            context.actorOf(FetchService.props(
              remote   = cluster.getBroker(node).get,
              clientId = clientId,
              request  = req
          ), "fetcher-"+node)
        }
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
                case PartitionMessages(partition, infos) ⇒
                  log.info(s"commit partition = ${partition}, offset = ${infos.last.offset}")
                  PartitionOffsetCommitRequest(partition, infos.last.offset)
              })
          }))

        stay
      }

      if(msgs.size > 0) sendCommit
    }

    private def decodeFetchedMsgs(msg: FetchResponse) = {
      FetchedMessages.decode(0, msg)
    }

    private def onMessageFetched(msg: FetchResponse) = {
      log.info("fetch response received")

      val commits = decodeFetchedMsgs(msg)
      // handle

      autoCommit(commits.msgs)

      stay
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

