package io.darwin.afka.packets.common

import io.darwin.kafka.macros.KafkaPacketElement

/**
  * Created by darwin on 29/12/2016.
  */
@KafkaPacketElement
case class ProtoMessage
  ( crc        : Int,
    magic      : Byte,
    attributes : Byte,
    timestamp  : Long,
    key        : Array[Byte],
    value      : Array[Byte])

@KafkaPacketElement
case class ProtoMessageInfo
  ( offset  : Long,
    msgSize : Int,
    msg     : ProtoMessage)

@KafkaPacketElement
case class ProtoMessageSet
  ( msgs: Array[ProtoMessageInfo] )
