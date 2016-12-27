package io.darwin.afka.packets.responses

import io.darwin.kafka.macros.{KafkaResponseElement, KafkaResponsePacket}

/**
  * Created by darwin on 27/12/2016.
  */
@KafkaResponseElement
case class Coordinator
  ( nodeId : Int,
    host   : String,
    port   : Int)

@KafkaResponsePacket
case class GroupCoordinateResponse
  ( error: Short,
    coordinator: Coordinator)
