package io.darwin.afka.packets.responses

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

/**
  * Created by darwin on 25/12/2016.
  */
class KafkaResponse(chan: SocketChannel) {

  val size: Int = {
    val buf = ByteBuffer.allocate(4)
    chan.read(buf)
  }

}
