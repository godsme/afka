package io.darwin.afka.akka

import java.net.InetSocketAddress
import java.nio.ByteBuffer

import akka.io.{IO, Tcp}
import akka.actor.{Actor, ActorRef, FSM}
import akka.util.ByteString
import io.darwin.afka.packets.requests.{ConsumerGroupReqMeta, GroupProtocol, JoinGroupRequest}

import scala.concurrent.duration._

/**
  * Created by darwin on 26/12/2016.
  */

///////////////////////////////////////////////////////////////////////
object GroupCoordinator{

  sealed trait State
  case object DISCONNECTED extends State
  case object CONNECTING   extends State
  case object JOINING      extends State
  case object JOINED       extends State

  sealed trait Data
  case object Dummy extends Data
}

///////////////////////////////////////////////////////////////////////
class GroupCoordinator(remote: InetSocketAddress, topics: Array[String], keepAlive: Int)
  extends FSM[GroupCoordinator.State, GroupCoordinator.Data] {
  import Tcp._

  import GroupCoordinator._

  var requestCorrelation: Int = -1
  var lastCorrelation: Int = 0
  var client: ActorRef = _
  var socket: Option[ActorRef] = None

  private def connect = {
    client = context.actorOf(KafkaNetworkClient.props(remote = remote, owner = self), "client")
  }

  override def preStart() = {
    connect
  }

  private def joinGroup = {
    def getGroupMeta: ByteBuffer = {
      val consumerMeta = ConsumerGroupReqMeta(subscription = topics, userData = ByteBuffer.allocate(0))
      ByteStringSinkChannel().encodeWithoutSize(consumerMeta).toByteBuffer
    }

    def getProtocols: Array[GroupProtocol] = {
      Array(GroupProtocol(name = "range", metaData = getGroupMeta))
    }

    val req = JoinGroupRequest( groupId = "darwin-group", protocols = getProtocols)
    socket.get ! Write(ByteStringSinkChannel().encode(req, lastCorrelation, ""))
  }

  private def joined(data: ByteString) {}

  startWith(CONNECTING, Dummy)

  when(DISCONNECTED, stateTimeout = 60 second) {
    case Event(StateTimeout, Dummy) => {
      connect
      goto(CONNECTING)
    }
  }

  when(CONNECTING, stateTimeout = 60 second) {
    case Event(KafkaClientConnected(conn: ActorRef), Dummy)  =>
      socket = Some(conn)
      joinGroup
      goto(JOINING)
  }

  when(JOINING) {
    case Event(KafkaResponseData(data: ByteString), Dummy) => {
      joined(data)
      goto(JOINED)
    }
  }

  when(JOINED, stateTimeout = keepAlive second) {
    case Event(StateTimeout, Dummy) => {
      // send keep alive
      stay
    }
  }

  whenUnhandled {
    case Event(_: ConnectionClosed, Dummy) =>
      goto(DISCONNECTED)

    case Event(_, _) =>
      stay
  }

  initialize()
}
