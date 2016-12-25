import java.net.InetSocketAddress
import java.nio.channels.SocketChannel

import io.darwin.afka.encoder.{ScatteredSinkByteBuffer, encoding}
import io.darwin.afka.packets.requests.{KafkaRequest, MetaDataRequest}

/**
  * Created by darwin on 25/12/2016.
  */
object Main extends App {

  val host: String = "localhost"
  val port: Int    = 9092

  var channel = SocketChannel.open(new InetSocketAddress(host, port))
  println("connected")

  var buf = ScatteredSinkByteBuffer()

  val req: KafkaRequest = MetaDataRequest()

  req.encode(buf, 1, "abc")

  val bufs = buf.get

  println(s"write ${channel.write(bufs)}")

  Thread.sleep(1000)

  channel.close()
}
