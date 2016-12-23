package io.darwin.macros

import scala.annotation.compileTimeOnly
import scala.meta._

/**
  * Created by darwin on 24/12/2016.
  */
@compileTimeOnly("kafka request element encoder generator")
class KafkaRequestElement extends scala.annotation.StaticAnnotation {
  inline def apply(defn: Any): Any = meta {
    defn
  }
}
