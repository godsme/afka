package io.darwin.afka.packets.requests

import java.nio.channels.Pipe.SinkChannel

/**
  * Created by darwin on 25/12/2016.
  */
trait KafkaRequest {
  def encode(chan: SinkChannel, correlationId: Int, clientId: String)
}
