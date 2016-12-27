import java.net.InetSocketAddress

import akka.actor.{ActorSystem, Props}
import io.darwin.afka.akka.{BootStrap, MetaDataService}

/**
  * Created by darwin on 25/12/2016.
  */
object Main extends App {

  val host: String = "localhost"
  val port: Int = 9092

  val system = ActorSystem("push-service")
  //implicit val ec = system.dispatcher
  //implicit val materializer = ActorMaterializer()

  val bootstrp = system.actorOf(Props[BootStrap], "bootstrap")
  val metaService = system.actorOf(MetaDataService.props(remote = new InetSocketAddress("localhost", 9092), bootstrp), "meta-service")

  Thread.sleep(1000)

  system.terminate()
}
