package io.darwin.afka.packets.requests

import akka.util.ByteString
import io.darwin.kafka.macros.{KafkaRequestElement, KafkaRequestPacket}

/**
  * Created by darwin on 27/12/2016.
  */
@KafkaRequestElement
case class GroupAssignment
  ( member     : String,
    assignment : ByteString)

@KafkaRequestPacket(apiKey = 14, version = 0)
case class SyncGroupRequest
  ( groupId    : String,
    generation : Int,
    member     : String,
    assignment : Array[GroupAssignment] = Array.empty)
