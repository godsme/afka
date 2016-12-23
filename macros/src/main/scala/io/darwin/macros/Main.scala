package io.darwin.macros

import scala.annotation.compileTimeOnly
import scala.meta._

/**
  * Created by darwin on 23/12/2016.
  */
@compileTimeOnly("enable macro paradise to expand macro annotations")
class Main extends scala.annotation.StaticAnnotation {
  inline def apply(defn: Any): Any = meta {
    defn match {
      case q"object $name { ..$stats }" =>
        val main = q"def main(args: Array[String]): Unit = { ..$stats }"
        q"object $name { $main }"
      case _ =>
        abort("@main must annotate an object.")
    }
  }
}

