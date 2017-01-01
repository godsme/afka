import java.net.InetSocketAddress

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import io.darwin.afka.services.{BootStrap, ClusterService, GroupCoordinator, MetaDataService}

/**
  * Created by darwin on 25/12/2016.
  */
object Main extends App {

  val config = ConfigFactory.load()

  val getBootStrap: Array[InetSocketAddress] = {
    val boots = config.getConfigList("afka.bootstrap")
    for(i ← Array.range(0, boots.size()))
      yield new InetSocketAddress(boots.get(i).getString("host"), boots.get(i).getInt("port"))
  }


//  val host: String = boot.getString("host")
//  val port: Int = boot.getInt("port")

  val system = ActorSystem("push-service")

  val bootstrap = system.actorOf(ClusterService.props(clusterId="1", bootstraps=getBootStrap), "cluster")
//  val metaService = system.actorOf( MetaDataService.props
//      ( remote   = new InetSocketAddress(host, port),
//        clientId = "darwin-client",
//        groupId  = "darwin-group",
//        topics   = Array("godsme-3", "godsme-4"))
//      , "darwin")


}
