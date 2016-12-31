package io.darwin.afka.packets.responses

import io.darwin.afka.packets.common.ProtoMemberAssignment
import io.darwin.kafka.macros.KafkaResponsePacket

/**
  * Created by darwin on 27/12/2016.
  */
@KafkaResponsePacket
case class SyncGroupResponse
  ( error      : Short,
    assignment : Option[ProtoMemberAssignment])
