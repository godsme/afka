package io.darwin.macros

import scala.annotation.compileTimeOnly
import scala.meta._
/**
  * Created by darwin on 24/12/2016.
  */

@compileTimeOnly("kafka request encoder generator")
class KafkaRequest(key: Int) extends scala.annotation.StaticAnnotation {
  inline def apply(defn: Any): Any = meta {

    def parseParams(p: Term.Param) = p match {
      case param"..$mods $paramname: $tpeopt = $expropt" => {
        val t: Option[Type.Arg] = tpeopt

        println(t.get.toString)
      }
      case _ => throw new Exception("invalid parameter")
    }

    defn match {
      case q"..$mods class $tname[..$tparams] ..$ctorMods (...$paramss) extends $template" => {
        paramss.flatten.foreach(s => parseParams(s))
        defn
      }
      case _ => throw new Exception("kafka request should be defined as a case class")
    }
  }
}
