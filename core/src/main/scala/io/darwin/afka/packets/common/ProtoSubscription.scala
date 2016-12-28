package io.darwin.afka.packets.common

import akka.util.ByteString
import io.darwin.kafka.macros.KafkaPacketElement

/**
  * Created by darwin on 27/12/2016.
  */
@KafkaPacketElement
case class ProtoSubscription
  ( version      : Short = 0,
    topics       : Array[String],
    userData     : ByteString = ByteString.empty)
