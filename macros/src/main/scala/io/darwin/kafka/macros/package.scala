package io.darwin.kafka

import scala.collection.immutable.Seq
import scala.meta.{Defn, _}

/**
  * Created by darwin on 24/12/2016.
  */
package object macros {

//  private val wrapperTypes: Array[String] = Array("Option", "Array")
//
//  def isValidWrapperType(tp: String): Boolean = {
//    wrapperTypes.contains(tp)
//  }
//
//  private val predefinedTypes: Array[String] =
//    Array("Boolean","Byte","Short","Int","Long","String","ByteBuffer")
//
//  def isPredefinedType(tp: String): Boolean = {
//    predefinedTypes.contains(tp)
//  }

  def createImports: Stat = {
    q"import io.darwin.afka.decoder.{ArrayDecoder, KafkaDecoder, SourceChannel, decoding}"
  }

  def createDecoderObject(name: Type.Name, paramss: Seq[Seq[Term.Param]]): Defn.Object = {
    val args = paramss.map(_.map(param => Term.Arg.Named(
      Term.Name(param.name.value), Term.Apply(
        Term.ApplyType(Term.Name("decoding"), Seq(Type.Name(param.decltpe.get.toString))), Seq(Term.Name("chan"))))))

    val decoderName = name.toString + "Decoder"
    q"""
         implicit object ${Term.Name(decoderName)} extends KafkaDecoder[$name] {
            override def decode(chan: SourceChannel): $name = {
               ${Ctor.Ref.Name(name.value)}(...$args)
            }
         }
       """
  }

  def getPatVarTerm(name: String): Pat.Var.Term = {
    Pat.Var.Term(Term.Name(name))
  }

  def createArrayDecoder(name: Type.Name): Defn.Val = {
    q"""
       implicit val ${getPatVarTerm("ARRAY_OF_" + name.toString)} = ArrayDecoder.make[${Type.Name(name.toString)}]
     """
  }

  def createNullArrayDecoder(name: Type.Name): Defn.Val = {
    q"""
       implicit val ${getPatVarTerm("NULLARRAY_OF_" + name.toString)} = ArrayDecoder.makeNullable[${Type.Name(name.toString)}]
     """
  }

  def createDecoders(name: Type.Name, paramss: Seq[Seq[Term.Param]]) : Seq[Stat]= {
    val imp = createImports
    val decoder = createDecoderObject(name, paramss)
    val arrayDecoder = createArrayDecoder(name)
    val nullArrayDecoder = createNullArrayDecoder(name)
    Seq(imp, q"$decoder", q"$arrayDecoder", q"$nullArrayDecoder")
  }

  def createPacketDecoder(name: Type.Name, paramss: Seq[Seq[Term.Param]]): Seq[Stat] = {
    val imp = createImports
    val decoder = createDecoderObject(name, paramss)
    Seq(imp, q"$decoder")
  }

  def insertToObject(seqn: Seq[Stat], cls: Defn.Class, obj: Defn.Object): Term.Block = {
    val seqs: Seq[Stat] = seqn ++: obj.templ.stats.getOrElse(Nil)
    val newObj = obj.copy(
      templ = obj.templ.copy(stats = Some(seqs)))
    Term.Block(Seq(cls, newObj))
  }

  def generateCompanion(stats: Seq[Stat], cls: Defn.Class, name: Type.Name): Term.Block = {
    val companion   = q"object ${Term.Name(name.value)} { ..$stats }"
    Term.Block(Seq(cls, companion))
  }
}
