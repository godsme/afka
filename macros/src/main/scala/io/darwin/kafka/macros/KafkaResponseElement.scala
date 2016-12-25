package io.darwin.kafka.macros


import scala.collection.immutable.Seq
import scala.annotation.compileTimeOnly
import scala.meta._

/**
  * Created by darwin on 24/12/2016.
  */
@compileTimeOnly("kafka response element encoder generator")
class KafkaResponseElement extends scala.annotation.StaticAnnotation {
  inline def apply(defn: Any): Any = meta {
    defn match {
      case Term.Block(
            Seq(cls @ Defn.Class(_, name, _, ctor, _),
            companion: Defn.Object)) => {
        insertToObject(createDecoders(name, ctor.paramss), cls, companion)
      }
      case cls @ Defn.Class(_, name, _, ctor, _) => {
        generateCompanion(createDecoders(name, ctor.paramss), cls, name)
      }
      case _ =>
        println(defn.structure)
        abort("@KafkaResponseElement should be defined as a case class")
    }
  }
}
