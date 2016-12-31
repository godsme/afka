package io.darwin.afka.packets.common

import akka.util.ByteString
import io.darwin.kafka.macros.{KafkaPacketElement}

/**
  * Created by darwin on 27/12/2016.
  */
@KafkaPacketElement
case class ProtoPartitionAssignment
  ( topic      : String,
    partitions : Array[Int])


@KafkaPacketElement
case class ProtoMemberAssignment
  ( version    : Short = 0,
    topics     : Array[ProtoPartitionAssignment],
    userData   : ByteString = ByteString.empty)

