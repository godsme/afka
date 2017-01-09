package io.darwin.afka.services

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Terminated}
import com.typesafe.config.ConfigFactory
import io.darwin.afka.services.domain.Consumer
import io.darwin.afka.services.pool.ClusterService
import io.darwin.afka.services.pool.ClusterService.ClusterChanged

import scala.collection.mutable.Map

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

  context.system.eventStream.subscribe(self, classOf[ClusterChanged])

  var consumers: Map[Int, ActorRef] = Map.empty

  val groups: Array[String] = Array.range(0, 20).map(i ⇒ s"group-${i}")

  val base = 20

  def startConsumer(n: Int) = {
    val consumer = context.actorOf(Consumer.props(
      cluster, groups(n/base), Array(s"todo-${n}", s"todo-${n+1}")), n.toString)
    context watch consumer

    consumers += n → consumer
  }

  def startConsumers = {
    for(i ← 0 until 100)
      startConsumer(i)
  }

  override def receive: Receive = {
    case ClusterChanged() ⇒
      if(consumers.isEmpty)
        startConsumers

    case Terminated(c) ⇒
      consumers -= c.path.name.toInt
      startConsumer(c.path.name.toInt)

    case e ⇒ log.info(s"event ${e} received ............")
  }
}

