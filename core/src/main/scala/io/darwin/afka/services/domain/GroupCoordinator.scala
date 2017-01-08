package io.darwin.afka.services.domain

import java.net.InetSocketAddress

import akka.actor.FSM.Failure
import akka.actor.{ActorRef, FSM, Props, Terminated}
import io.darwin.afka.assignors.PartitionAssignor.MemberSubscription
import io.darwin.afka.assignors.RangeAssignor
import io.darwin.afka.decoder.decode
import io.darwin.afka.domain.GroupOffsets.GroupOffsets
import io.darwin.afka.domain.KafkaCluster
import io.darwin.afka.encoder.encode
import io.darwin.afka.packets.common.{ProtoMemberAssignment, ProtoPartitionAssignment, ProtoSubscription}
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses.{SyncGroupResponse, _}
import io.darwin.afka.services.common._

import scala.collection.mutable.Map
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
  case object ASSIGN       extends State
  case object PHASE2       extends State
  case object JOINED       extends State

  sealed trait Data
  case object Dummy extends Data
  case class  Subscription(v: Array[MemberSubscription]) extends Data
  case class  Offsets(v: Option[GroupOffsets]) extends Data

  trait Actor extends FSM[State, Data] {
    this: KafkaServiceSinkChannel{
      val topics  : Array[String]
      val clientId: String
      val groupId : String
      val cluster : KafkaCluster
    } ⇒

    private var memberId   : Option[String] = None
    private var generation : Int            = 0

    private def suicide(reason: String): State = {
      log.error(s"suicide: $reason")
      stop(Failure(reason))
    }

    startWith(CONNECTING, Dummy)

    when(DISCONNECTED, stateTimeout = 60 second) {
      case Event(StateTimeout, Dummy)            ⇒ goto(CONNECTING)
    }

    when(CONNECTING, stateTimeout = 5 second) {
      case Event(ChannelConnected(_), _)   ⇒ goto(PHASE1)
    }

    when(PHASE1) {
      case Event(r: JoinGroupResponse, _)  ⇒ onJoined(r)
    }

    when(ASSIGN) {
      case Event(r: MetaDataResponse, Subscription(sub)) ⇒ onMetaDataReceived(r, sub)
    }

    when(PHASE2) {
      case Event(r: SyncGroupResponse, _)    ⇒ onResult(r.error, "SyncGroup")(onSync(r))
    }

    when(JOINED, stateTimeout = 5 second) {
      case Event(StateTimeout, _)                           ⇒ heartBeat
      case Event(r: OffsetFetchResponse, Offsets(Some(o)))  ⇒ onOffsetFetched(r, o)
      case Event(_: OffsetFetchResponse, Offsets(None))     ⇒ stay
      case Event(r: OffsetCommitResponse, _)                ⇒ onCommitted(r)
      case Event(r: HeartBeatResponse, _)                   ⇒ onHeartBeatRsp(r)
    }

    /////////////////////////////////////////////////////////////////
    private var heartbeatAck = true

    private def heartBeat = {
      if(heartbeatAck) {
        sending(HeartBeatRequest(groupId, generation, memberId.get))
        heartbeatAck = false
        stay
      }
      else
        suicide("heart beat with response")
    }

    private def onHeartBeatRsp(r: HeartBeatResponse): State = {
      heartbeatAck = true
      onResult(r.error, "HeartBeat")(stay)
    }

    /////////////////////////////////////////////////////////////////
    private val assigner = new RangeAssignor

    private def sendJoinRequest = {
      sending(JoinGroupRequest(
        groupId   = groupId,
        memberId  = memberId.getOrElse(""),
        protocols = Array(GroupProtocol(
          name = assigner.name,
          meta = encode(assigner.subscribe(topics))))))
    }

    private def rejoinGroup(cause: Short, on: String): State = {
      log.info(s"re-join group when ${on}, cause=${cause}")
      goto(PHASE1)
    }

    private def onJoined(rsp: JoinGroupResponse) = {
      log.info(
        s"@joinGroup: error = ${rsp.errorCode}, " +
          s"generation = ${rsp.generation}, " +
          s"proto = ${rsp.groupProtocol}, " +
          s"leader = ${rsp.leaderId}, " +
          s"member = ${rsp.memberId}, " +
          s"members = ${rsp.members.getOrElse(Array.empty).mkString(",")}")

      onResult(rsp.errorCode, "JoinGroup") {
        generation = rsp.generation
        memberId = Some(rsp.memberId)
        syncGroup(rsp)
      }
    }

    private def getTopics(subs: Array[MemberSubscription]) = {
      val topicMap: Map[String, String] = Map.empty

      subs.foreach { s ⇒
        s.subscriptions.topics.foreach { topic ⇒
          log.info(s"member = ${s.memberId} topic=${topic}")
          topicMap.getOrElseUpdate(topic, topic)
        }
      }

      topicMap.toArray.map{case (k, _) ⇒ k}
    }

    private def decodeSubscription(rsp: JoinGroupResponse) = {
      rsp.members.get.map { case GroupMember(id, meta) ⇒
        MemberSubscription(id, decode[ProtoSubscription](meta))
      }
    }

    private def sync(assignments: Array[GroupAssignment] = Array.empty): State = {
      sending(SyncGroupRequest(groupId, generation, memberId.get, assignments))
      goto(PHASE2) using Dummy
    }

    private def onMetaDataReceived(r: MetaDataResponse, subscription: Array[MemberSubscription]) = {
      def assignments(c: KafkaCluster) = {
        assigner
          .assign(c, subscription).toArray
          .map { case (member, ms) ⇒
            GroupAssignment(
              member     = member,
              assignment = encode(ProtoMemberAssignment(topics = ms.toArray)))
          }
      }

      sync(assignments(KafkaCluster(r)))
    }

    private def syncGroup(rsp: JoinGroupResponse): State = {
      def assigning(subscription: Array[MemberSubscription]) = {
        sending(MetaDataRequest(Some(getTopics(subscription))))
        goto(ASSIGN) using(Subscription(subscription))
      }

      if(rsp.leaderId == rsp.memberId)
        assigning(decodeSubscription(rsp))
      else
        sync()
    }

    private def onSync(r: SyncGroupResponse) = {
      def logMsg = {
        r.assignment.map { s ⇒
          s.topics.foreach { case ProtoPartitionAssignment(topic, partitions) ⇒
            log.info(s"topic=${topic}, partitions={${partitions.mkString(", ")}}")
          }
        }
      }

      val groupOffsets = r.assignment.map { case p ⇒
        new GroupOffsets(cluster, p.topics)
      }

      def fetchOffset = {
        logMsg

        r.assignment.fold(()) { s ⇒
          sending(OffsetFetchRequest(
            group  = groupId,
            topics = s.topics))
        }

        goto(JOINED) using(Offsets(groupOffsets))
      }


      onResult(r.error, "@SyncGroup")(fetchOffset)
    }

    private var fetchers: Array[ActorRef] = Array.empty

    private def stopFetchers = {
      fetchers.foreach(context.stop)
      fetchers = Array.empty
    }

    private def onOffsetFetched(offsets: OffsetFetchResponse, groupOffsets: GroupOffsets) = {
      groupOffsets.update(offsets.topics)

      fetchers = groupOffsets.map { (node, off) ⇒
        context watch context.actorOf(FetchService.props(
            channel  = cluster.getBroker(node).get,
            clientId = clientId,
            offsets  = off ),
          "fetcher-"+node)
      }

      stay using(Dummy)
    }

//    def autoCommit(msgs: MutableList[TopicMessages]) = {
//      def getPartitions(p: MutableList[PartitionMessages]) =
//        p.toArray.map { case PartitionMessages(partition, _, Some(info)) ⇒
//          PartitionOffsetCommitRequest(partition, info.last.offset)
//        }
//
//      def getTopics = msgs.toArray.map { case TopicMessages(topic, m) ⇒
//          TopicOffsetCommitRequest(
//            topic      = topic,
//            partitions = getPartitions(m))
//        }
//
//      def sendCommit = {
//        sending(OffsetCommitRequest(
//          groupId    = groupId,
//          generation = generation,
//          consumerId = memberId.get,
//          topics     = getTopics ))
//
//        stay
//      }
//
//      if(msgs.size > 0) sendCommit
//    }

    private def onCommitted(commit: OffsetCommitResponse) = {
      commit.topics.foreach {
        case OffsetCommitTopicResponse(topic, partitions) ⇒
          log.info(s"commit topic: ${topic}")
          partitions.foreach { case OffsetCommitPartitionResponse(partition, error) ⇒
            log.info(s"partition = ${partition}, error=${error}")
          }
      }

      stay
    }


    private def onResult(error: Short, on: String)(success: ⇒ State) = {
      error match {
        case KafkaErrorCode.NO_ERROR               ⇒ success
        case KafkaErrorCode.UNKNOWN_MEMBER_ID      ⇒ { memberId = None; rejoinGroup(error, on)}

        case KafkaErrorCode.ILLEGAL_GENERATION |
             KafkaErrorCode.REBALANCE_IN_PROGRESS  ⇒ rejoinGroup(error, on)

        case KafkaErrorCode.GROUP_COORDINATOR_NOT_AVAILABLE |
             KafkaErrorCode.NOT_COORDINATOR_FOR_GROUP          ⇒ suicide(error.toString)

        case KafkaErrorCode.GROUP_AUTHORIZATION_FAILED         ⇒ suicide(error.toString)
        case e ⇒ suicide(s"${on} failed: ${e}")
      }
    }

    ////////////////////////////////////////////////////////////////////
    onTransition {
      case _ -> PHASE1 ⇒
        stopFetchers
        sendJoinRequest
    }

    whenUnhandled {
      case Event(_: Terminated, Dummy) ⇒
        stopFetchers
        goto(DISCONNECTED)
      case Event(_: NotReady, _)   ⇒ stop
      case Event(_: Unreachable, _) ⇒ {
        log.error("connection to coordinator server lost")
        stop
      }
      case Event(e, _) ⇒
        log.info(s"unhandled: ${e}")
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
  extends GroupCoordinator.Actor with KafkaService {

  val remote = new InetSocketAddress(coordinator.host, coordinator.port)
}

