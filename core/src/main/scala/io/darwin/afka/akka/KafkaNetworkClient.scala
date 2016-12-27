package io.darwin.afka.akka

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
    private var size: Option[Int] = None

    def buffer(packet: ByteString) = {
      def getCached(packet: ByteString): ByteString = {
        if(cached.isEmpty) packet else cached.get ++ packet
      }

      cached = Some(getCached(packet))

      process
    }

    private def process = {
      def getRemain(remain: ByteString) = {
        if(remain.size == 0) None else Some(remain)
      }

      if(size.isEmpty && cached.get.length >= 4) {
        size = Some(cached.get.iterator.getInt)
      }

      if(size.isDefined) {
        val s = size.get + 4
        val d = cached.get

        if(s <= d.length) {
          owner ! KafkaResponseData(d.slice(4, s))
          cached = getRemain(d.drop(s))
          if(cached.isDefined) {
            log.info(s"cached size = ${cached.get.length}")
          }
        }
      }
    }
  }

  private val cached = new Cache()

  private def bufferWriting(packet: ByteString) = {
    unsent :+= packet
  }

  private def suicide = context stop self

  case object Ack extends Event

  private def acknowledge(conn: ActorRef) = {
    require(!unsent.isEmpty)

    unsent = unsent.drop(1)

    if(unsent.isEmpty ) {
      if(closing) suicide
    }
    else {
      conn ! Write(unsent(0), Ack)
    }
  }

  override def receive: Receive = {
    case CommandFailed(_: Connect) =>
      log.error(s"connecting to ${remote.toString} failed!")
      suicide

    case c @ Connected(_, _)  =>
      log.info(s"connected to ${remote.toString}.")

      val connection = sender()
      owner ! KafkaClientConnected(connection)
      sender() ! Register(self, keepOpenOnPeerClosed = true)
      context.become({
        case Received(data) => cached.buffer(data)
        case Ack            => acknowledge(connection)
        case PeerClosed     => closing = true
        case CommandFailed(Write(data: ByteString, _)) => bufferWriting(data)
        case _: ConnectionClosed => suicide
      }, discardOld = false)

  }

  override val supervisorStrategy = OneForOneStrategy(loggingEnabled = false) {
    case e =>
      log.error("Unexpected exception {}", e.getMessage)
      Stop
  }
}