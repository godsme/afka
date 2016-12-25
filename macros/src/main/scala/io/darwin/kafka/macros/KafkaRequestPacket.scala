package io.darwin.kafka.macros

import scala.annotation.compileTimeOnly
import scala.collection.immutable.Seq
import scala.meta.{abort, _}
/**
  * Created by darwin on 24/12/2016.
  */

@compileTimeOnly("kafka request encoder generator")
class KafkaRequestPacket(apiKey: Int, version: Int = 0) extends scala.annotation.StaticAnnotation {
  inline def apply(defn: Any): Any = meta {

    def getEncodingCodes(paramss: Seq[Seq[Term.Param]]): Seq[Term.Apply] = {
      paramss.flatten.map{ param =>
        q"encoding(chan, ${Term.Name(param.name.value)})"
      }
    }

    defn match {
      case cls @ Defn.Class(_, name, _, ctor, _) => {
        val q"new $_(..${args})" = this

        var thisMap = Map[String, Term.Arg]()

        args.foreach( arg => arg match {
          case Term.Arg.Named(key, value) => {
            thisMap += key.toString() -> value
          }
        })

        val apiKeyValue:  Term.Arg = thisMap.get("apiKey").getOrElse(abort("no apiKey"))
        val versionValue: Term.Arg = thisMap.get("version").getOrElse(abort("no version"))

        val cons: Term.Apply =
          q"""
            ${Ctor.Ref.Name("RequestHeader")}($apiKeyValue, $versionValue, correlationId, clientId).encode(chan)
          """

        val applySeq = getEncodingCodes(ctor.paramss).+:(cons)

        val encoding =
          q"""
           def encode(chan: SinkChannel, correlationId: Int, clientId: String) = {
             ..$applySeq
           }
         """

        val imports = createEncoderImports

        val newStats: Seq[Stat] = Seq(q"$imports", q"$encoding") ++: cls.templ.stats.getOrElse(Nil)

        val newCls = cls.copy(templ = cls.templ.copy(stats = Some(newStats)))

        println(newCls.toString())

        newCls
      }
      case _ => throw new Exception("kafka request should be defined as a case class")
    }
  }
}