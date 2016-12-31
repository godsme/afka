package io.darwin.afka.decoder

/**
  * Created by darwin on 24/12/2016.
  */
class NullableDecoder[A](decoder: WithSizeDecoder[A])
  extends WithSizeDecoder[Option[A]] {

  override def decode(chan: SourceChannel, size: Int): Option[A] = {
    if(size <= 0) None
    else Some(decoder.decode(chan, size))
  }
}

object NullableDecoder {

  class SkipSizeDecoder[A](implicit decoder: KafkaDecoder[A])
    extends WithSizeDecoder[Option[A]]{
    override def decode(chan: SourceChannel, size: Int): Option[A] = {
      if(size <= 0) None
      else Some(decoder.decode(chan))
    }
  }

  def make[A](implicit decoder: KafkaDecoder[A]): KafkaDecoder[Option[A]] =
    new VarSizeDecoder[Option[A]](_.getInt)(new SkipSizeDecoder[A])
}
