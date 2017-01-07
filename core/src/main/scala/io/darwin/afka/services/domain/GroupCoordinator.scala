package io.darwin.afka.services.domain

import java.net.InetSocketAddress

import akka.actor.FSM.Failure
import akka.actor.{ActorRef, FSM, Props, Terminated}
import io.darwin.afka.assignors.PartitionAssignor.MemberSubscription
import io.darwin.afka.assignors.RangeAssignor
import io.darwin.afka.decoder.decode
import io.darwin.afka.domain.FetchedMessages.{PartitionMessages, TopicMessages}
import io.darwin.afka.domain.GroupOffsets.GroupOffsets
import io.darwin.afka.domain.KafkaCluster
import io.darwin.afka.encoder.encode
import io.darwin.afka.packets.common.{ProtoMemberAssignment, ProtoPartitionAssignment, ProtoSubscription}
import io.darwin.afka.packets.requests._
import io.darwin.afka.packets.responses.{SyncGroupResponse, _}
import io.darwin.afka.services.common._
import scala.collection.mutable.Map

import io.darwin.afka.services.pool.PoolDynamicSinkChannel

import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern._
import akka.util.Timeout

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
  case object ASSIGN       extends State
  case object PHASE2       extends State
  case object JOINED       extends State

  sealed trait Data
  case object Dummy extends Data
  case class  Subscription(v: Array[MemberSubscription]) extends Data

  trait Actor extends FSM[State, Data] {
    this: KafkaServiceSinkChannel{
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

    var server: Option[ActorRef] = None

    when(DISCONNECTED, stateTimeout = 60 second) {
      case Event(StateTimeout, Dummy)            ⇒ goto(CONNECTING)
    }

    when(CONNECTING, stateTimeout = 5 second) {
      case Event(ChannelConnected(who), Dummy) ⇒
        server = Some(who)
        joinGroup
    }

    when(PHASE1) {
      case Event(r: JoinGroupResponse, Dummy)    ⇒ onJoined(r)
    }

    when(ASSIGN) {
      case Event(r: MetaDataResponse, Subscription(sub)) ⇒
        onMetaDataReceived(r, sub)
    }

    when(PHASE2) {
      case Event(r: SyncGroupResponse, Dummy)    ⇒
        onResult(r.error, "SyncGroup")(onSync(r))
    }

    var heartbeatAck = true
    when(JOINED, stateTimeout = 5 second) {
      case Event(StateTimeout, Dummy)            ⇒ heartBeat
      case Event(r: OffsetFetchResponse, Dummy)  ⇒ onOffsetFetched(r)
      case Event(r: OffsetCommitResponse, Dummy) ⇒ onCommitted(r)
      case Event(r: HeartBeatResponse, Dummy)    ⇒
        heartbeatAck = true
        onResult(r.error, "HeartBeat")(stay)
    }

    /////////////////////////////////////////////////////////////////
    val assigner = new RangeAssignor

    private def sendJoinRequest = {
      sending(JoinGroupRequest(
        groupId = groupId,
        memberId = memberId.getOrElse(""),
        protocols = Array(GroupProtocol(
          name = assigner.name,
          meta = encode(assigner.subscribe(topics))))))
    }

    private def joinGroup: State = {
      sendJoinRequest
      goto(PHASE1)
    }

    private def rejoinGroup(cause: Short, on: String): State = {
      log.info(s"re-join group when ${on}, cause=${cause}")

      stopFetchers
      sendJoinRequest
      goto(PHASE1) using(Dummy)
    }

    private def onJoined(rsp: JoinGroupResponse) = {
      def onSuccess = {
        generation = rsp.generation
        memberId = Some(rsp.memberId)

        sync(rsp)
      }

      log.info(
        s"@joinGroup: error = ${rsp.errorCode}, " +
          s"generation = ${rsp.generation}, " +
          s"proto = ${rsp.groupProtocol}, " +
          s"leader = ${rsp.leaderId}, " +
          s"member = ${rsp.memberId}, " +
          s"members = ${rsp.members.getOrElse(Array.empty).mkString(",")}")

      onResult(rsp.errorCode, "JoinGroup")(onSuccess)
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

    private def startSync(assignments: Array[GroupAssignment] = Array.empty): State = {
      sending(SyncGroupRequest(groupId, generation, memberId.get, assignments))
      goto(PHASE2) using Dummy
    }

    private def onMetaDataReceived(r: MetaDataResponse, subscription: Array[MemberSubscription]) = {
      def leaderSync(c: KafkaCluster) = {
        assigner
          .assign(c, subscription).toArray
          .map { case (member, assignments) ⇒
            GroupAssignment(
              member     = member,
              assignment = encode(ProtoMemberAssignment(topics = assignments.toArray)))
          }
      }

      startSync(leaderSync(KafkaCluster(r)))
    }

    private def sync(rsp: JoinGroupResponse): State = {
      if(rsp.leaderId == rsp.memberId) {
        val subscription = decodeSubscription(rsp)
        sending(MetaDataRequest(Some(getTopics(subscription))))
        goto(ASSIGN) using(Subscription(subscription))
      }
      else
        startSync()
    }

    var groups: Option[GroupOffsets] = None

    private def onSync(r: SyncGroupResponse) = {
      groups = r.assignment.map { case p ⇒
        new GroupOffsets(cluster, p.topics)
      }

      def logMsg = {
        r.assignment.map { s ⇒
          s.topics.foreach { case ProtoPartitionAssignment(topic, partitions) ⇒
            log.info(s"topic=${topic}, partitions={${partitions.mkString(", ")}}")
          }
        }
      }

      def fetchOffset = {
        logMsg

        r.assignment.fold(()) { s ⇒
          sending(OffsetFetchRequest(
            group  = groupId,
            topics = s.topics))
        }

        goto(JOINED)
      }

      onResult(r.error, "@SyncGroup")(fetchOffset)
    }

    var fetchers: Array[ActorRef] = Array.empty

    private def stopFetchers = fetchers.foreach(context.stop)

    private def onOffsetFetched(offsets: OffsetFetchResponse) = {
      groups.foreach(_.update(offsets.topics))

      fetchers = groups.get.offsets.toArray.map { case (node, off) ⇒

        context.actorOf(FetchService.props(
            channel  = cluster.getBroker(node).get,
            clientId = clientId,
            offsets  = off ),
          "fetcher-"+node)
      }

      stay
    }

    def autoCommit(msgs: MutableList[TopicMessages]) = {
      def getPartitions(p: MutableList[PartitionMessages]) =
        p.toArray.map { case PartitionMessages(partition, _, Some(info)) ⇒
          PartitionOffsetCommitRequest(partition, info.last.offset)
        }

      def getTopics = msgs.toArray.map { case TopicMessages(topic, m) ⇒
          TopicOffsetCommitRequest(
            topic      = topic,
            partitions = getPartitions(m))
        }

      def sendCommit = {
        sending(OffsetCommitRequest(
          groupId    = groupId,
          generation = generation,
          consumerId = memberId.get,
          topics     = getTopics ))

        stay
      }

      if(msgs.size > 0) sendCommit
    }

    def onCommitted(commit: OffsetCommitResponse) = {
      commit.topics.foreach {
        case OffsetCommitTopicResponse(topic, partitions) ⇒
          log.info(s"commit topic: ${topic}")
          partitions.foreach { case OffsetCommitPartitionResponse(partition, error) ⇒
            log.info(s"partition = ${partition}, error=${error}")
          }
      }

      stay
    }

    private def heartBeat = {
      if(heartbeatAck) {
        sending(HeartBeatRequest(groupId, generation, memberId.get))
        heartbeatAck = false
        stay
      }
      else
        suicide("heart beat with response")
    }

    def onResult(error: Short, on: String)(success: ⇒ State): State = {
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
    whenUnhandled {
      case Event(_: Terminated, Dummy) ⇒
        stopFetchers
        goto(DISCONNECTED)
      case Event(e: NotReady, _) ⇒
        stop
      case Event(_:Unreachable, _) ⇒ {
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

