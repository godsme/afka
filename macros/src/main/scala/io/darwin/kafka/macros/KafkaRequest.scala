package io.darwin.kafka.macros

import scala.annotation.compileTimeOnly
import scala.meta._
/**
  * Created by darwin on 24/12/2016.
  */

@compileTimeOnly("kafka request encoder generator")
class KafkaRequest(apiKey: Int, version: Int = 0) extends scala.annotation.StaticAnnotation {
  inline def apply(defn: Any): Any = meta {

    def findRootType(tp: Tree): String = {
      if(tp.children.size == 0) tp.toString
      else if(tp.children.size != 2) {
        throw new Exception("Invalid Type")
      }
      else{
        val children = tp.children.toArray
        if(!isValidWrapperType(children(0).toString)) {
          throw new Exception("Unknown Type Wrapper")
        }
        else {
          findRootType(children(1))
        }
      }
    }

    def parseParams(p: Term.Param) = p match {
      case param"..$mods $paramname: $tpeopt = $expropt" => {
        if(tpeopt.isEmpty) throw new Exception("no type")
        val tp: Type.Arg = tpeopt.get

        println(tp.toString + ":" + tp.children.size)

        println("root type = " + findRootType(tp))
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
