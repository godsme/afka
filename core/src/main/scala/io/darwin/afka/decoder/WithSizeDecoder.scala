package io.darwin.afka.decoder

/**
  * Created by darwin on 24/12/2016.
  */
trait WithSizeDecoder[A] {
  def decode(chan: SourceChannel, size: Int): A
}
