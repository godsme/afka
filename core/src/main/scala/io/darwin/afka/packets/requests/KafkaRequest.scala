package io.darwin.afka.packets.requests

import io.darwin.afka.encoder.SinkChannel

/**
  * Created by darwin on 25/12/2016.
  */
trait KafkaRequest {
  def apiKey:  Short
  def version: Short

  def encode(chan: SinkChannel, correlationId: Int, clientId: String)
}
