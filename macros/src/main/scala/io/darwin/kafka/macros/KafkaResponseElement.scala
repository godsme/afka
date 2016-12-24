package io.darwin.kafka.macros


import scala.annotation.compileTimeOnly
import scala.meta._

/**
  * Created by darwin on 24/12/2016.
  */
@compileTimeOnly("kafka response element encoder generator")
class KafkaResponseElement extends scala.annotation.StaticAnnotation {
  inline def apply(defn: Any): Any = meta {
    defn
  }
}
