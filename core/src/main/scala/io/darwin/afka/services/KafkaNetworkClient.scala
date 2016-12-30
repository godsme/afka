package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props}
import akka.io.{IO, Tcp}
import akka.util.ByteString

/**
  * Created by darwin on 26/12/2016.
  */
object KafkaNetworkClient {
  def props(remote: InetSocketAddress, owner: ActorRef) =
    Props(classOf[KafkaNetworkClient], remote, owner)
}

case class KafkaClientConnected(connection: ActorRef)
case class KafkaResponseData(data: ByteString)

class KafkaNetworkClient(remote: InetSocketAddress, owner: ActorRef)
  extends Actor with ActorLogging {

  import Tcp._


  override def preStart(): Unit = {
    import context.system

    IO(Tcp) ! Connect(remote, options = Vector(SO.TcpNoDelay(false)))
    log.info("connecting ...")
  }


  private var closing = false
  private var unsent = Vector.empty[ByteString]


  class Cache {
    private var cached: Option[ByteString] = None

    def buffer(packet: ByteString) = {
      def getCached(packet: ByteString): ByteString = {
        if(cached.isEmpty) packet else cached.get ++ packet
      }

      log.info(s"response received ${packet}")

      cached = Some(getCached(packet))

      process
    }

    private def process = {
      def getRemain(remain: ByteString) = {
        if(remain.size == 0) None else Some(remain)
      }

      def getCachedSize = {
        if(cached.get.length >= 4) cached.get.iterator.getInt else 0
      }

      def trySend(size: Int) = {
        val s = size + 4
        val d = cached.get

        if(s <= d.length) {
          owner ! KafkaResponseData(d.slice(4, s))
          cached = getRemain(d.drop(s))
        }
      }

      def sending(size: Int) = if(size > 0) trySend(size)

      sending(getCachedSize)
    }
  }


  private val cached = new Cache()

  private def bufferWriting(packet: ByteString) = unsent :+= packet

  private def suicide(reason: String) = {
    log.info(s"suicide for ${reason}")
    context stop self
  }

  case object Ack extends Event

  private def acknowledge(conn: ActorRef) = {
    require(!unsent.isEmpty)

    unsent = unsent.drop(1)

    if(unsent.isEmpty) {
      if(closing) suicide("BUG: data inconsistency")
    }
    else {
      conn ! Write(unsent(0), Ack)
    }
  }

  override def receive: Receive = {
    case CommandFailed(_: Connect) ⇒
      suicide(s"server ${remote} unreachable")

    case Connected(_, _)  ⇒
      log.info(s"connected to ${remote}.")

      val connection = sender()
      owner ! KafkaClientConnected(connection)
      sender() ! Register(self, keepOpenOnPeerClosed = true)
      context.become({
        case Received(data) ⇒ cached.buffer(data)
        case Ack            ⇒ acknowledge(connection)
        case PeerClosed     ⇒ closing = true
        case CommandFailed(Write(data: ByteString, _)) ⇒ bufferWriting(data)
        case _: ConnectionClosed ⇒ suicide(s"connection to ${remote} lost")
      }, discardOld = false)

  }

  override val supervisorStrategy = OneForOneStrategy(loggingEnabled = false) {
    case e ⇒
      log.error("Unexpected exception {}", e.getMessage)
      Stop
  }
}
