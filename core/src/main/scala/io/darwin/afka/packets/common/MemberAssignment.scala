package io.darwin.afka.packets.common

import akka.util.ByteString
import io.darwin.kafka.macros.{KafkaPacketElement}

/**
  * Created by darwin on 27/12/2016.
  */
@KafkaPacketElement
case class PartitionAssignment
( topic      : String,
  partitions : Array[Int],
  userData   : ByteString = ByteString.empty)


@KafkaPacketElement
case class MemberAssignment
( version    : Short,
  assignment : Array[PartitionAssignment])

