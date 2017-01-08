package io.darwin.afka.decoder

import scala.reflect.ClassTag

/**
  * Created by darwin on 24/12/2016.
  */

object ArrayDecoder {

  private class WithSizeArrayDecoder[A: ClassTag](decoder: KafkaDecoder[A])
    extends WithSizeObjectDecoder[Array[A]] {

    override def doDecode(chan: SourceChannel, size: Int): Array[A] = {
      val r = new Array[A](size)

      var i = 0;
      while(i < size) {
        r(i) = decoder.decode(chan)
        i += 1
      }

      r
    }
  }

  private class WithSizeNullableArrayDecoder[A](decoder: WithSizeDecoder[Array[A]])
    extends NullableDecoder[Array[A]](decoder)

  def make[A: ClassTag](implicit decoder: KafkaDecoder[A]): KafkaDecoder[Array[A]] = {
    new VarSizeDecoder[Array[A]](_.getInt)(new WithSizeArrayDecoder[A](decoder))
  }

  def makeNullable[A: ClassTag](implicit decoder: KafkaDecoder[A]): KafkaDecoder[Option[Array[A]]] = {
    new VarSizeDecoder[Option[Array[A]]](_.getInt)(new NullableDecoder[Array[A]](new WithSizeArrayDecoder[A](decoder)))
  }
}

