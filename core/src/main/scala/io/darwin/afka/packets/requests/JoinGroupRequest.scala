package io.darwin.afka.packets.requests

import java.nio.ByteBuffer

import io.darwin.afka.encoder.{KafkaEncoder, SinkChannel, _}
import io.darwin.kafka.macros.{KafkaRequestPacket, KafkaRequestElement}

/**
  * Created by darwin on 24/12/2016.
  */
@KafkaRequestElement
case class GroupProtocol
  ( name:     String,
    metaData: ByteBuffer)


object GroupProtocol {

  implicit object GroupProtocolEncoder extends KafkaEncoder[GroupProtocol] {
    override def encode(ch: SinkChannel, o: GroupProtocol) = {
      encoding(ch, o.name)
      encoding(ch, o.metaData)
    }
  }

  implicit object ArrayOfGroupProtocolEncoder extends ArrayEncoder[GroupProtocol]
  implicit object NullableArrayOfGroupProtocolEncoder extends NullableArrayEncoder[GroupProtocol]
}

@KafkaRequestPacket(apiKey = 11, version = 1)
case class JoinGroupRequest
  ( groupId:          String,
    sessionTimeout:   Int,
    rebalanceTimeout: Int,
    memberId:         String = "",
    protocolType:     String,
    protocols:        Array[GroupProtocol]) {

  def encode(chan: SinkChannel, correlationId: Int, clientId: String) {
      RequestHeader(11, 1, correlationId, clientId).encode(chan)
      encoding(chan, groupId)
      encoding(chan, sessionTimeout)
      encoding(chan, rebalanceTimeout)
      encoding(chan, memberId)
      encoding(chan, protocolType)
      encoding(chan, protocols)
  }
}

