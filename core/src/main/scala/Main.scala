import java.net.InetSocketAddress

import akka.actor.{ActorSystem, Props}
import io.darwin.afka.akka.{BootStrap, GroupCoordinator, MetaDataService}

/**
  * Created by darwin on 25/12/2016.
  */
object Main extends App {

  val host: String = "localhost"
  val port: Int = 9092

  val system = ActorSystem("push-service")

  val bootstrap = system.actorOf(Props[BootStrap], "bootstrap")
  val metaService = system.actorOf(
    GroupCoordinator.props( remote = new InetSocketAddress("localhost", 9092),
      topics = Array("my-topic", "darwin")),
    "meta-service")
}
