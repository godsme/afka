package io.darwin.afka.macros

import scala.annotation.compileTimeOnly
import scala.collection.immutable.Seq
import scala.meta.{Defn, _}

/**
  * Created by darwin on 11/1/2017.
  */
@compileTimeOnly("@ActorObject actor properties object generator")
class ActorObject extends scala.annotation.StaticAnnotation {
  inline def apply(defn: Any): Any = meta {
    def generateProps(name: Type.Name, paramss: Seq[Seq[Term.Param]]) = {
      val params = paramss.map { _.map {param ⇒
        Term.Param(mods=Seq(), param.name, param.decltpe, param.default)
      }}

      val args = paramss.flatten.map( param =>
        arg"${Term.Name(param.name.value)}"
      )

      val prop =
        q"""
         def props(...$params) = akka.actor.Props(classOf[$name] ..$args)
         """

      Seq(prop)
    }

    def insertToObject(seqn: Seq[Stat], obj: Defn.Object) = {
      val seqs: Seq[Stat] = seqn ++: obj.templ.stats.getOrElse(Nil)
      obj.copy(templ = obj.templ.copy(stats = Some(seqs)))
    }

    def combine(cls: Defn.Class, companion: Defn.Object): Term.Block = {
      val v = Term.Block(Seq(cls, companion))
      println(v.toString)
      v
    }

    def generate(cls: Defn.Class)(companion: ⇒ Defn.Object) = {
      val Defn.Class(_, name, _, ctor, _) = cls
      val props = generateProps(name, ctor.paramss)
      val newObj = insertToObject(props, companion)
      combine(cls, newObj)
    }

    defn match {
      case Term.Block(Seq(cls: Defn.Class, companion: Defn.Object)) => {
        generate(cls)(companion)
      }
      case cls@Defn.Class(_, name, _, _, _) => {
        generate(cls) {
          q"object ${Term.Name(name.value)} {}"
        }
      }
      case _ =>
        println(defn.structure)
        abort("@ActorObject should be defined as a class")
    }
  }
}