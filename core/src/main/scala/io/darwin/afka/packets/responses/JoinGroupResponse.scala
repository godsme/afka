package io.darwin.afka.packets.responses

import akka.util.ByteString
import io.darwin.kafka.macros.{KafkaResponseElement, KafkaResponsePacket}

/**
  * Created by darwin on 27/12/2016.
  */

@KafkaResponseElement
case class GroupMember
  ( memberId : String,
    meta     : ByteString)

@KafkaResponsePacket
case class JoinGroupResponse
  (errorCode     : Short,
   generation   : Int,
   groupProtocol : String,
   leaderId      : String,
   memberId      : String,
   members       : Array[GroupMember])
