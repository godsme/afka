package io.darwin.afka.packets.requests

import io.darwin.kafka.macros.KafkaRequestPacket

/**
  * Created by darwin on 27/12/2016.
  */

@KafkaRequestPacket(apiKey = 12, version = 0)
case class HeartBeatRequest
  ( groupId    : String,
    generation : Int,
    memberId   : String)
