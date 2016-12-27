package io.darwin.afka.packets.responses

import akka.util.ByteString
import io.darwin.kafka.macros.KafkaResponsePacket

/**
  * Created by darwin on 27/12/2016.
  */
@KafkaResponsePacket
case class SyncGroupResponse
  ( error      : Short,
    assignment : ByteString)
