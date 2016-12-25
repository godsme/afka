package io.darwin.kafka.macros

import scala.annotation.compileTimeOnly
import scala.meta._
/**
  * Created by darwin on 24/12/2016.
  */

@compileTimeOnly("kafka response encoder generator")
class KafkaResponsePacket extends scala.annotation.StaticAnnotation {
  inline def apply(defn: Any): Any = meta {
    defn match {
      case Term.Block(
      Seq(cls @ Defn.Class(_, name, _, ctor, _),
      companion: Defn.Object)) => {
        val r = insertToObject(createPacketDecoder(name, ctor.paramss), cls, companion)
        println(r.toString())
        r
      }
      case cls @ Defn.Class(_, name, _, ctor, _) => {
        val r = generateCompanion(createPacketDecoder(name, ctor.paramss), cls, name)
        println(r.toString())
        r
      }
      case _ =>
        println(defn.structure)
        abort("@KafkaRequest should be defined as a case class")
    }
  }
}