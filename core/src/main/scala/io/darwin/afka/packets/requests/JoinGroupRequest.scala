package io.darwin.afka.packets.requests

import akka.util.ByteString
import io.darwin.kafka.macros.{KafkaRequestElement, KafkaRequestPacket}

/**
  * Created by darwin on 24/12/2016.
  */
@KafkaRequestElement
case class GroupProtocol
  ( name:     String = "range",     // "range"/"roundrobin"
    meta: ByteString )


@KafkaRequestPacket(apiKey = 11, version = 1)
case class JoinGroupRequest
  ( groupId:          String,
    sessionTimeout:   Int = 10000,
    rebalanceTimeout: Int = 10000,
    memberId:         String = "",
    protocolType:     String = "consumer",
    protocols:        Array[GroupProtocol])


