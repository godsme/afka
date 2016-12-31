package io.darwin.afka.packets.requests

import io.darwin.afka.packets.common.ProtoPartitionAssignment
import io.darwin.kafka.macros.{KafkaRequestPacket}

/**
  * Created by darwin on 30/12/2016.
  */
@KafkaRequestPacket(apiKey = 9, version = 1)
case class OffsetFetchRequest
  ( group  : String,
    topics : Array[ProtoPartitionAssignment])

