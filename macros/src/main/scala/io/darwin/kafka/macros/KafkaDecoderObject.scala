package io.darwin.kafka.macros

import scala.annotation.compileTimeOnly
import scala.meta._
/**
  * Created by darwin on 24/12/2016.
  */

@compileTimeOnly("kafka request encoder generator")
class KafkaDecoderObject extends scala.annotation.StaticAnnotation {
  inline def apply(defn: Any): Any = meta {
    println(defn.structure)
    defn
  }
}