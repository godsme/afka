package io.darwin.afka.packets.requests

import java.nio.ByteBuffer

import io.darwin.afka.encoder.{KafkaEncoder, SinkChannel, _}
import io.darwin.kafka.macros.{KafkaRequestPacket, KafkaRequestElement}

/**
  * Created by darwin on 24/12/2016.
  */
@KafkaRequestElement
case class GroupProtocol
  ( name:     String = "range",     // "range"/"roundrobin"
    metaData: ByteBuffer )


@KafkaRequestPacket(apiKey = 11, version = 1)
case class JoinGroupRequest
  ( groupId:          String,
    sessionTimeout:   Int = 1000000,
    rebalanceTimeout: Int = 1000000,
    memberId:         String = "",
    protocolType:     String = "consumer",
    protocols:        Array[GroupProtocol])


