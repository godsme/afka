package io.darwin.afka.akka

import java.net.InetSocketAddress

import akka.io.{IO, Tcp}
import akka.actor.{Actor, ActorRef, FSM}

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
  case object CONNECTED    extends State

  sealed trait Data
  case object Dummy extends Data
}

///////////////////////////////////////////////////////////////////////
class GroupCoordinator(remote: InetSocketAddress, keepAlive: Int)
  extends FSM[GroupCoordinator.State, GroupCoordinator.Data] {
  import Tcp._
  import context.system

  import GroupCoordinator._

  var requestCorrelation: Int = -1
  var lastCorrelation: Int = 0

  private def connect = {
    IO(Tcp) ! Connect(remote, options = Vector(SO.TcpNoDelay(false)))
  }

  override def preStart() = {
    connect
  }

  startWith(CONNECTING, Dummy)

  when(DISCONNECTED, stateTimeout = 60 second) {
    case Event(StateTimeout, Dummy) => {
      connect
      goto(CONNECTING)
    }
  }

  when(CONNECTING, stateTimeout = 60 second) {
    case Event(CommandFailed(_: Connect), Dummy) =>
      goto(DISCONNECTED) using Dummy
    case Event(c @ Connected(_, _), Dummy)  =>
      // send join group
      goto(JOINING)
  }

  when(JOINING) {
    case Event(Received(data), Dummy) => {
      // process joining
      goto(CONNECTED)
    }
  }

  when(CONNECTED, stateTimeout = keepAlive second) {
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
