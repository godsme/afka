package io.darwin.afka.packets.requests

import io.darwin.afka.encoder.{SinkChannel, encoding}

/**
  * Created by darwin on 25/12/2016.
  */
case class RequestHeader
  ( val apiKey        : Short,
    val apiVersion    : Short,
    val correlationId : Int,
    val clientId      : String) {

  def encode(chan: SinkChannel) = {
    encoding(chan, apiKey)
    encoding(chan, apiVersion)
    encoding(chan, correlationId)
    encoding(chan, clientId)
  }
}

