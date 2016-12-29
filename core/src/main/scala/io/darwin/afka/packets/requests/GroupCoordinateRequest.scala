package io.darwin.afka.packets.requests

import io.darwin.kafka.macros.KafkaRequestPacket

/**
  * Created by darwin on 27/12/2016.
  */
@KafkaRequestPacket(apiKey = 10, version = 0)
case class GroupCoordinateRequest
  ( group: String )
