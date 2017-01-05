package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Terminated}
import com.typesafe.config.ConfigFactory
import io.darwin.afka.services.domain.Consumer
import io.darwin.afka.services.pool.ClusterService
import io.darwin.afka.services.pool.ClusterService.ClusterReady

/**
  * Created by darwin on 2/1/2017.
  */

class PushService
  extends Actor with ActorLogging {

  val config = ConfigFactory.load()

  val getBootStrap: Array[InetSocketAddress] = {
    val boots = config.getConfigList("afka.bootstrap")
    for(i ← Array.range(0, boots.size()))
      yield new InetSocketAddress(boots.get(i).getString("host"), boots.get(i).getInt("port"))
  }

  val cluster = context.actorOf(
    ClusterService.props(
      clusterId  = "1",
      bootstraps = getBootStrap,
      self),
    "cluster")
  context watch cluster

  var consumer: ActorRef = null

  def startConsumer = {
    consumer = context.actorOf(Consumer.props("darwin-group", Array("godsme-5", "godsme-6")), "consumer")
    context watch consumer
  }

  override def receive: Receive = {
    case ClusterReady ⇒ {
      if(consumer == null) {
        startConsumer
      }
    }
    case Terminated(c) ⇒
      if(c == consumer) {
        startConsumer
      }
    case e ⇒ log.info(s"event ${e} received ............")
  }
}

