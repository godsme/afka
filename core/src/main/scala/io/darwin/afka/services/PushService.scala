package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.typesafe.config.ConfigFactory
import io.darwin.afka.services.ClusterService.ClusterReady

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

  override def receive: Receive = {
    case ClusterReady ⇒ {
      consumer = context.actorOf(Consumer.props("darwin-group", Array("godsme-3", "godsme-4")), "consumer")
      context watch consumer
    }

    case e ⇒ log.info(s"event ${e} received ............")
  }
}

