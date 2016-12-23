package io.darwin.macros

import scala.meta.Type
import scala.meta.classifiers.Classifier

/**
  * Created by darwin on 24/12/2016.
  */
object TypeClassifier {
  implicit val ByteClassifier = new Classifier[Type.Arg, Byte] {
    override def apply(x: Type.Arg): Boolean = x.toString == "Byte"
  }
}
