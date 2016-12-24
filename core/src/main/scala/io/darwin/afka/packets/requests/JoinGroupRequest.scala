package io.darwin.afka.packets.requests

import java.nio.ByteBuffer

import io.darwin.afka.encoder.{KafkaEncoder, SinkChannel, _}
import io.darwin.kafka.macros.{KafkaRequest, KafkaRequestElement}

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


@KafkaRequest(apiKey = 11, version = 1)
case class JoinGroupRequest
  ( groupId:          String,
    sessionTimeout:   Int,
    rebalanceTimeout: Int,
    memberId:         String = "",
    protocolType:     String,
    protocols:        Array[GroupProtocol])


object JoinGroupRequestObject {

  implicit object JoinGroupRequestEncoder extends KafkaEncoder[JoinGroupRequest] {
    override def encode(ch: SinkChannel, o: JoinGroupRequest) = {
      encoding(ch, o.groupId)
      encoding(ch, o.sessionTimeout)
      encoding(ch, o.rebalanceTimeout)
      encoding(ch, o.memberId)
      encoding(ch, o.protocolType)
      encoding(ch, o.protocols)
    }
  }

}
