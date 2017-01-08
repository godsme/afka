package io.darwin.afka.decoder

/**
  * Created by darwin on 24/12/2016.
  */
class VarSizeDecoder[A]( read: SourceChannel => Int )
                       ( implicit decoder: WithSizeDecoder[A] )
  extends KafkaDecoder[A] {
  override def decode(ch: SourceChannel): A = decoder.decode(ch, read(ch))
}

