package io.darwin.afka.packets.responses

import io.darwin.kafka.macros.KafkaResponsePacket

/**
  * Created by darwin on 27/12/2016.
  */
@KafkaResponsePacket
case class HeartBeatResponse
  ( error: Short )
