import akka.actor.{ActorSystem, Props}
import io.darwin.afka.services.PushService
import io.darwin.afka.services.domain.DeadLetterListener
/**
  * Created by darwin on 25/12/2016.
  */
object Main extends App {

  implicit val system = ActorSystem()
  implicit val ec = system.dispatcher

  val bootstrap = system.actorOf(Props[PushService],"push-service")
  //val deadLetter = system.actorOf(Props[DeadLetterListener], "dead-letter")

  //  val metaService = system.actorOf( MetaDataService.props
//      ( remote   = new InetSocketAddress(host, port),
//        clientId = "darwin-client",
//        groupId  = "darwin-group",
//        topics   = Array("godsme-3", "godsme-4"))
//      , "darwin")

//  implicit val timeout: Timeout = FiniteDuration(10, TimeUnit.SECONDS)
//  implicit val materializer = ActorMaterializer()
//  Thread.sleep(1000)
//  println("wake up")
//  bootstrap ? CreateConsumer("darwin-group", Array("godsme-3", "godsme-4"))

}
