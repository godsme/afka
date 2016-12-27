package io.darwin.afka.packets.requests

import java.nio.ByteBuffer

import io.darwin.kafka.macros.KafkaRequestElement

/**
  * Created by darwin on 27/12/2016.
  */

@KafkaRequestElement
case class ConsumerGroupReqMeta
  ( version      : Short = 0,
    subscription : Array[String],
    userData     : ByteBuffer )

